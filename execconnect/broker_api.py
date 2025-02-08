#!/usr/bin/env python3
"""
broker_api.py

This module handles connectivity to the broker/exchange API and provides a function to send orders
with retry logic and standardized JSON logging.
"""

import os
import json
import time
import logging
import traceback
import uuid
from datetime import datetime
from typing import Dict

# Attempt to import the Alpaca API; if unavailable, fall back to simulation.
try:
    import alpaca_trade_api as tradeapi
except ImportError:
    tradeapi = None

# Import network-related exceptions (example using pika's exceptions)
from pika.exceptions import AMQPConnectionError, AMQPChannelError

logger = logging.getLogger("ExecConnect.BrokerAPI")
logger.setLevel(logging.INFO)

ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY")
ALPACA_BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")


def send_order(trade: Dict, max_retries: int = 3) -> Dict:
    """
    Sends an order to the broker API using Alpaca (if configured).
    Includes retry logic for transient connectivity issues.

    Parameters:
        trade (dict): A dictionary containing trade details.
        max_retries (int): Maximum number of retry attempts (default is 3).

    Returns:
        dict: Execution result with order details.
    """
    attempt = 1
    while attempt <= max_retries:
        try:
            log_msg = {
                "timestamp": datetime.utcnow().isoformat(),
                "level": "INFO",
                "service": "ExecConnect.BrokerAPI",
                "message": f"Sending order, attempt {attempt}",
                "trade": trade,
                "correlation_id": trade.get("correlation_id")
            }
            logger.info(json.dumps(log_msg))

            # If the broker API is not configured, simulate an order.
            if not tradeapi or not ALPACA_API_KEY:
                return simulate_order(trade)

            api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url=ALPACA_BASE_URL)
            order = api.submit_order(
                symbol=trade['symbol'],
                qty=trade['quantity'],
                side=trade['action'].lower(),
                type='limit',
                limit_price=trade['price'],
                time_in_force='gtc'
            )
            execution_result = {
                "order_id": order.id,
                "status": order.status.upper(),
                "filled_quantity": int(order.filled_qty),
                "avg_price": float(order.filled_avg_price) if order.filled_avg_price else trade['price'],
                "timestamp": datetime.utcnow().isoformat(),
                "remaining_quantity": trade['quantity'] - int(order.filled_qty)
            }
            log_msg = {
                "timestamp": datetime.utcnow().isoformat(),
                "level": "INFO",
                "service": "ExecConnect.BrokerAPI",
                "message": "Order sent successfully",
                "execution_result": execution_result,
                "correlation_id": trade.get("correlation_id")
            }
            logger.info(json.dumps(log_msg))
            return execution_result

        except (AMQPConnectionError, AMQPChannelError) as e:
            delay = 2 ** attempt  # Exponential backoff: 2, 4, 8 seconds.
            log_msg = {
                "timestamp": datetime.utcnow().isoformat(),
                "level": "ERROR",
                "service": "ExecConnect.BrokerAPI",
                "message": f"Broker connectivity error on attempt {attempt}",
                "error": str(e),
                "stack_trace": traceback.format_exc(),
                "retry_delay": delay,
                "correlation_id": trade.get("correlation_id")
            }
            logger.error(json.dumps(log_msg))
            time.sleep(delay)
            attempt += 1

        except Exception as e:
            log_msg = {
                "timestamp": datetime.utcnow().isoformat(),
                "level": "ERROR",
                "service": "ExecConnect.BrokerAPI",
                "message": f"Unexpected error sending order: {str(e)}",
                "stack_trace": traceback.format_exc(),
                "correlation_id": trade.get("correlation_id")
            }
            logger.error(json.dumps(log_msg))
            raise

    # If all retries fail, log a final error and return a safe default.
    default_result = {
        "order_id": "N/A",
        "status": "FAILED",
        "filled_quantity": 0,
        "avg_price": 0.0,
        "timestamp": datetime.utcnow().isoformat(),
        "remaining_quantity": trade.get("quantity", 0)
    }
    log_msg = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": "ERROR",
        "service": "ExecConnect.BrokerAPI",
        "message": "All broker API retries failed",
        "default_result": default_result,
        "correlation_id": trade.get("correlation_id")
    }
    logger.error(json.dumps(log_msg))
    return default_result


def simulate_order(trade: Dict) -> Dict:
    """
    Simulate an order execution when the broker API is not configured.

    Parameters:
        trade (dict): A dictionary containing trade details.

    Returns:
        dict: Simulated execution result.
    """
    trade_id = f"SIM-{uuid.uuid4()}"
    status = "FILLED" if trade.get("risk_approved", False) else "FAILED"
    filled_qty = trade["quantity"] if status == "FILLED" else 0
    simulated_result = {
        "order_id": trade_id,
        "status": status,
        "filled_quantity": filled_qty,
        "avg_price": trade["price"] + (0.05 if status == "FILLED" and random.choice([True, False]) else 0.0),
        "timestamp": datetime.utcnow().isoformat(),
        "remaining_quantity": 0 if status == "FILLED" else trade.get("quantity", 0)
    }
    log_msg = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": "INFO",
        "service": "ExecConnect.BrokerAPI",
        "message": "Simulated order execution",
        "execution_result": simulated_result,
        "correlation_id": trade.get("correlation_id")
    }
    logger.info(json.dumps(log_msg))
    return simulated_result