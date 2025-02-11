# consumer.py
import json
import time
import traceback
from datetime import datetime
from typing import Dict, Any, List
import time
import pika
from common.logging_helper import get_logger

# Initialize the logger with the appropriate service name
logger = get_logger("Quant")

def on_message(channel, method, properties, body):
    # Extract correlation_id if present
    correlation_id = properties.headers.get('correlation_id') if properties and properties.headers else 'UNKNOWN'
    logger.info("Received event", extra={'correlation_id': correlation_id})
    # Acknowledge the message
    channel.basic_ack(delivery_tag=method.delivery_tag)
from .strategies import (
    SymbolStrategy, MeanReversionMomentumStrategy, MLSignalStrategy,
    MACDStrategy, BollingerBandsStrategy, RSIIndicator, aggregate_signals
)
from .common import process_market_data, compute_signal, publish_trade_signal, get_rabbitmq_connection
from .performance import PerformanceTracker

# Global load metrics.
message_count = 0
load_start_time = time.time()

import os

STRATEGY_TYPE = os.environ.get("STRATEGY_TYPE", "SYMBOL").upper()
MA_SHORT = int(os.environ.get("MA_SHORT", "2"))
MA_LONG = int(os.environ.get("MA_LONG", "5"))
RSI_PERIOD = int(os.environ.get("RSI_PERIOD", "14"))
USE_RSI_CONFIRMATION = os.environ.get("USE_RSI_CONFIRMATION", "false").lower() in ("true", "1", "yes")
RSI_BUY_THRESHOLD = float(os.environ.get("RSI_BUY_THRESHOLD", "30"))
RSI_SELL_THRESHOLD = float(os.environ.get("RSI_SELL_THRESHOLD", "70"))
ML_WINDOW = int(os.environ.get("ML_WINDOW", "50"))
DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")


def run_consumer():
    global message_count, load_start_time
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue="market_data", durable=True)
    channel.queue_declare(queue="trade_signals", durable=True)

    postgres_logger = None
    if DB_HOST and DB_USER and DB_PASS and DB_NAME:
        try:
            from common import PostgresLogger
            postgres_logger = PostgresLogger(DB_HOST, DB_USER, DB_PASS, DB_NAME)
            log_json("INFO", "Connected to Postgres for signal logging.")
        except Exception:
            log_json("ERROR", "Failed to connect to Postgres; continuing without DB logging",
                     extra={"stack_trace": traceback.format_exc()})

    performance_tracker = PerformanceTracker(initial_cash=100000.0)
    strategies = {}

    def on_message(ch, method, properties, body) -> None:
        global message_count, load_start_time
        try:
            raw_data = json.loads(body)
            processed_data = process_market_data(raw_data)
            symbol = processed_data["symbol"]
            timestamp = processed_data["timestamp"]
            price = processed_data["price"]
            signals: List[Dict[str, Any]] = []
            if STRATEGY_TYPE == "AGGREGATED":
                if symbol not in strategies:
                    strategies[symbol] = {
                        "legacy": SymbolStrategy(symbol, MA_SHORT, MA_LONG, RSI_PERIOD,
                                                 USE_RSI_CONFIRMATION, RSI_BUY_THRESHOLD, RSI_SELL_THRESHOLD),
                        "MACD": MACDStrategy(symbol),
                        "Bollinger": BollingerBandsStrategy(symbol),
                        "ML": MLSignalStrategy(symbol, window=ML_WINDOW),
                        "RSI": RSIIndicator(period=RSI_PERIOD)
                    }
                for strat_name, strat in strategies[symbol].items():
                    if strat_name == "RSI":
                        prices_list = list(strategies[symbol]["legacy"].prices)
                        if prices_list:
                            rsi_val = strat.compute(prices_list)
                            # Option: Use RSI to adjust confidence.
                            if rsi_val < 30:
                                s = {"action": "BUY", "confidence": 1.0, "source": "RSI"}
                            elif rsi_val > 70:
                                s = {"action": "SELL", "confidence": 1.0, "source": "RSI"}
                            else:
                                s = {}
                        else:
                            s = {}
                    else:
                        s = strat.update(timestamp, price)
                    if s:
                        signals.append(s)
                final_signal = aggregate_signals(signals)
            else:
                if symbol not in strategies:
                    if STRATEGY_TYPE == "MRM":
                        from strategies import MeanReversionMomentumStrategy
                        strategies[symbol] = MeanReversionMomentumStrategy(symbol, window=10, threshold=0.01)
                    elif STRATEGY_TYPE == "ML":
                        strategies[symbol] = MLSignalStrategy(symbol, window=ML_WINDOW)
                    else:
                        strategies[symbol] = SymbolStrategy(symbol, MA_SHORT, MA_LONG, RSI_PERIOD,
                                                            USE_RSI_CONFIRMATION, RSI_BUY_THRESHOLD, RSI_SELL_THRESHOLD)
                final_signal = strategies[symbol].update(timestamp, price)
                if not final_signal:
                    final_signal = compute_signal(processed_data)
            if "fill_ratio" not in final_signal:
                final_signal["fill_ratio"] = raw_data.get("fill_ratio", 1.0)
            if final_signal and final_signal.get("signal_type") != "HOLD":
                publish_trade_signal(final_signal)
                log_json("INFO", "Published signal", extra={"signal": final_signal})
                if postgres_logger:
                    try:
                        postgres_logger.log_signal(final_signal)
                        log_json("DEBUG", "Signal logged to Postgres")
                    except Exception:
                        log_json("ERROR", "Failed to log signal to Postgres",
                                 extra={"stack_trace": traceback.format_exc()})
                performance_tracker.update(final_signal)
            message_count += 1
            now = time.time()
            if now - load_start_time >= 60:
                rate = message_count / (now - load_start_time)
                log_json("INFO", "Load metrics", extra={"messages_per_minute": round(rate, 2)})
                message_count = 0
                load_start_time = now
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            log_json("ERROR", "Error processing market data message", extra={"stack_trace": traceback.format_exc()})
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(queue="market_data", on_message_callback=on_message)
    log_json("INFO", "Waiting for market data. To exit press CTRL+C")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log_json("INFO", "Interrupted by user; shutting down...")
    finally:
        channel.stop_consuming()
        connection.close()
        if postgres_logger:
            postgres_logger.close()
        performance_tracker.report_performance()
        log_json("INFO", "Shutdown complete.")


if __name__ == "__main__":
    run_consumer()