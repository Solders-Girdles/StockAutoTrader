#!/usr/bin/env python3
"""
main.py

Main Entry Point for RiskOps Service:
- Receives trade signals from the durable "trade_signals" queue.
- Uses risk checks from risk.py to evaluate trade risk (multi-asset risk, exposure limits, circuit breakers,
  VaR, stress tests, and extended scenario codes).
- If approved, updates an in-memory portfolio and publishes the approved trade via mq_util.
- Implements a consumer that ACKs messages on successful processing and NACKs them on errors.
- Incorporates a retry/backoff loop to handle situations where RabbitMQ is slow to initialize.
- Logs every step with explicit JSON fields (timestamp, level, service, message, correlation_id).
"""

import os
import sys
import json
import time
from datetime import datetime, timezone
import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError

# Use the common logging helper instead of directly configuring the standard logging module.
from common.logging_helper import get_logger

# Use relative imports for internal modules.
from .risk import evaluate_trade
from .mq_util import publish_message

# Initialize the logger using the logging helper.
logger = get_logger("RiskOpsMain")

# Simulated portfolio data for risk checks.
portfolio_data = {
    "AAPL": 100,                # current shares held for AAPL
    "total_capital": 100000.0,    # total portfolio capital
    "available_margin": 50000.0,  # available margin in dollars
    "daily_drawdown_pct": 0.02    # current daily drawdown (2% loss)
}

def process_trade_signal(signal: dict, correlation_id: str = None) -> None:
    """
    Process a trade signal by evaluating risk, updating the portfolio, and publishing an approved trade if approved.

    Args:
        signal (dict): Trade signal with required keys.
        correlation_id (str): Optional correlation ID for tracing.
    """
    try:
        logger.info(json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "INFO",
            "service": "RiskOpsMain",
            "message": "Signal received.",
            "signal": signal,
            "correlation_id": correlation_id
        }))
        approved = evaluate_trade(signal, portfolio_data, correlation_id=correlation_id)
        if approved:
            if signal.get("action", "").upper() == "BUY":
                symbol = signal["symbol"]
                qty = int(signal.get("desired_quantity", 0))
                current = portfolio_data.get(symbol, 0)
                portfolio_data[symbol] = current + qty
                price = float(signal["price"])
                asset_class = signal.get("asset_class", "equity").lower()
                asset_params = {
                    "equity": {"leverage_limit": 2.0},
                    "options": {"leverage_limit": 1.5},
                    "futures": {"leverage_limit": 5.0},
                    "crypto": {"leverage_limit": 2.5}
                }
                leverage_limit = asset_params.get(asset_class, asset_params["equity"])["leverage_limit"]
                margin_used = (price * qty) / leverage_limit
                portfolio_data["available_margin"] -= margin_used
                logger.info(json.dumps({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "level": "INFO",
                    "service": "RiskOpsMain",
                    "message": "Portfolio updated.",
                    "symbol": symbol,
                    "new_position": portfolio_data[symbol],
                    "available_margin": portfolio_data["available_margin"],
                    "correlation_id": correlation_id
                }))
            approved_trade_message = {
                "symbol": signal["symbol"],
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action": "EXECUTE_BUY" if signal.get("action", "").upper() == "BUY" else "EXECUTE_SELL",
                "price": signal["price"],
                "quantity": int(signal.get("desired_quantity", 50)),
                "risk_approved": True,
                "correlation_id": correlation_id
            }
            publish_message("approved_trades", approved_trade_message, correlation_id=correlation_id)
        else:
            logger.info(json.dumps({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": "INFO",
                "service": "RiskOpsMain",
                "message": "Trade signal rejected by risk evaluation.",
                "signal": signal,
                "correlation_id": correlation_id
            }))
    except Exception as e:
        logger.exception(json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "ERROR",
            "service": "RiskOpsMain",
            "message": "Error processing trade signal.",
            "error": str(e),
            "correlation_id": correlation_id
        }))
        # Propagate exception so that the consumer can NACK the message.
        raise

def consume_trade_signals():
    """
    Set up a RabbitMQ consumer to process messages from the durable "trade_signals" queue.
    Implements a retry loop with exponential backoff to handle connection issues (e.g., RabbitMQ slow to initialize).

    Failover Strategy:
      - Partial Failover: On restart, pending messages in the durable queue are processed immediately.
      - Full Failover: If RiskOps is down, messages accumulate and are processed upon recovery.
      - Stale messages can be filtered via application logic or queue policies.
    """
    parameters = pika.ConnectionParameters(
        host=os.environ.get("RABBITMQ_HOST", "localhost"),
        credentials=pika.PlainCredentials(
            os.environ.get("RABBITMQ_USER", "guest"),
            os.environ.get("RABBITMQ_PASS", "guest")
        )
    )
    max_retries = 10
    retry_delay = 1
    for attempt in range(1, max_retries + 1):
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue="trade_signals", durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue="trade_signals", on_message_callback=on_message_callback)
            logger.info("Started consuming from 'trade_signals' queue.")
            channel.start_consuming()
            break  # Exit loop on successful consumption.
        except (AMQPConnectionError, AMQPChannelError) as e:
            logger.exception("RabbitMQ connection/channel error during consumer setup: %s", e)
            time.sleep(retry_delay)
            retry_delay *= 2
        except Exception as e:
            logger.exception("Unexpected error in consumer setup: %s", e)
            time.sleep(retry_delay)
            retry_delay *= 2

def on_message_callback(channel, method_frame, header_frame, body):
    """
    Callback for RabbitMQ consumer.
    Processes the incoming trade signal; ACKs if processing succeeds,
    otherwise NACKs the message (without requeueing) to route it to a dead-letter queue.
    """
    try:
        signal = json.loads(body)
        correlation_id = signal.get("correlation_id", "from_consumer")
        process_trade_signal(signal, correlation_id=correlation_id)
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    except Exception as e:
        logger.exception("Error processing message: %s", e)
        channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "consume":
        consume_trade_signals()
    else:
        # For demonstration, simulate processing a single trade signal.
        sample_signal = {
            "symbol": "AAPL",
            "timestamp": "2025-02-07T12:00:00Z",
            "action": "BUY",
            "price": 150.0,
            "desired_quantity": 50,
            "confidence": 0.80,
            "asset_class": "equity",
            "vix": 25
        }
        correlation_id = "abc123"
        process_trade_signal(sample_signal, correlation_id=correlation_id)
        # Use a relative import for reporting.
        from .reporting import generate_daily_risk_report
        report = generate_daily_risk_report(portfolio_data)
        logger.info(json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "INFO",
            "service": "RiskOpsMain",
            "message": "Daily risk report generated.",
            "report": report,
            "correlation_id": correlation_id
        }))

if __name__ == "__main__":
    main()