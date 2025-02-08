#!/usr/bin/env python3
"""
quant/main.py

A robust multi-symbol moving average crossover strategy integrated with RabbitMQ,
enhanced error handling, data schema validation, and optional Postgres logging.
This script consumes market data from the "market_data" queue, validates and processes
the data, applies configurable strategy logic (including MA crossover and optional RSI),
and publishes trading signals to the "trade_signals" queue.
Optionally, signals are logged into a Postgres table "quant_signals" for performance tracking.

Environment Variables:
    -- Strategy Parameters --
    MA_SHORT: Short moving average period (default: 2)
    MA_LONG: Long moving average period (default: 5)
    RSI_PERIOD: Period for RSI calculation (default: 14)
    USE_RSI_CONFIRMATION: "true" to require RSI confirmation, "false" otherwise (default: false)
    RSI_BUY_THRESHOLD: RSI threshold for BUY confirmation (default: 30)
    RSI_SELL_THRESHOLD: RSI threshold for SELL confirmation (default: 70)

    -- RabbitMQ Connection --
    RABBITMQ_HOST: RabbitMQ host (default: "localhost")
    RABBITMQ_USER: RabbitMQ username (default: "guest")
    RABBITMQ_PASS: RabbitMQ password (default: "guest")

    -- Optional Postgres Logging --
    DB_HOST, DB_USER, DB_PASS, DB_NAME
"""

import os
import json
import time
import logging
import random
import traceback
from datetime import datetime
from collections import deque
from typing import Dict, Any, Deque, Optional
import sys

import pika
import psycopg2

# Standard service name for logging.
SERVICE_NAME = "Quant"

# Configure basic logging (actual formatting is done in our helper function).
logging.basicConfig(level=logging.INFO, format="%(message)s")


def log_json(level: str, message: str, extra: Optional[Dict[str, Any]] = None):
    """
    Helper function to log JSON-formatted messages.
    Standard fields include: timestamp, level, service, and message.
    Optionally, extra fields (e.g., correlation_id) can be added.
    """
    if extra is None:
        extra = {}
    log_entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "level": level.upper(),
        "service": SERVICE_NAME,
        "message": message
    }
    log_entry.update(extra)
    if level.upper() == "ERROR":
        logging.error(json.dumps(log_entry))
    elif level.upper() == "WARNING":
        logging.warning(json.dumps(log_entry))
    elif level.upper() == "DEBUG":
        logging.debug(json.dumps(log_entry))
    else:
        logging.info(json.dumps(log_entry))


# Read strategy parameters from environment variables.
MA_SHORT = int(os.environ.get("MA_SHORT", "2"))
MA_LONG = int(os.environ.get("MA_LONG", "5"))
RSI_PERIOD = int(os.environ.get("RSI_PERIOD", "14"))
USE_RSI_CONFIRMATION = os.environ.get("USE_RSI_CONFIRMATION", "false").lower() in ("true", "1", "yes")
RSI_BUY_THRESHOLD = float(os.environ.get("RSI_BUY_THRESHOLD", "30"))
RSI_SELL_THRESHOLD = float(os.environ.get("RSI_SELL_THRESHOLD", "70"))

# Read Postgres environment variables (optional).
DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")


# ---------------------------
# Core Analysis & Validation
# ---------------------------
class SymbolStrategy:
    """
    Strategy for a single symbol using configurable moving averages and RSI indicator.
    Maintains a rolling window of prices and computes:
      - A short MA over the last MA_SHORT prices.
      - A long MA over the last MA_LONG prices.
      - An RSI over the last RSI_PERIOD price changes (if enough data).

    Generates a BUY or SELL signal when the short MA crosses the long MA,
    optionally confirmed by an RSI condition.
    """
    def __init__(self, symbol: str, ma_short: int, ma_long: int, rsi_period: int,
                 use_rsi: bool, rsi_buy: float, rsi_sell: float) -> None:
        self.symbol = symbol
        self.ma_short = ma_short
        self.ma_long = ma_long
        self.rsi_period = rsi_period
        self.use_rsi = use_rsi
        self.rsi_buy_threshold = rsi_buy
        self.rsi_sell_threshold = rsi_sell
        # Buffer size covers both MA and RSI needs.
        self.buffer_size = max(ma_long, rsi_period + 1)
        self.prices: Deque[float] = deque(maxlen=self.buffer_size)
        self.last_signal: str = ""

    def update(self, timestamp: str, price: float) -> Dict[str, Any]:
        """
        Update with a new price, compute indicators, and determine if a new signal is generated.

        Returns a dictionary with keys:
            - symbol (str)
            - timestamp (str, ISO8601)
            - price (float)
            - ma_short (float)
            - ma_long (float)
            - rsi (float, optional)
            - signal_type (str, legacy)
            - action (str)
            - signal (str)
            - confidence (float)
        """
        self.prices.append(price)
        result: Dict[str, Any] = {}
        if len(self.prices) < self.ma_long:
            return result

        prices_list = list(self.prices)
        short_prices = prices_list[-self.ma_short:]
        long_prices = prices_list[-self.ma_long:]
        ma_short_value = sum(short_prices) / self.ma_short
        ma_long_value = sum(long_prices) / self.ma_long

        if ma_short_value > ma_long_value:
            ma_signal = "BUY"
        elif ma_short_value < ma_long_value:
            ma_signal = "SELL"
        else:
            ma_signal = "HOLD"

        rsi_value: Optional[float] = None
        if len(prices_list) >= self.rsi_period + 1:
            diffs = [prices_list[i] - prices_list[i - 1] for i in range(1, len(prices_list))]
            recent_diffs = diffs[-self.rsi_period:]
            gains = [d if d > 0 else 0 for d in recent_diffs]
            losses = [-d if d < 0 else 0 for d in recent_diffs]
            avg_gain = sum(gains) / self.rsi_period
            avg_loss = sum(losses) / self.rsi_period
            if avg_loss == 0:
                rsi_value = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi_value = 100 - (100 / (1 + rs))

        final_signal = ma_signal
        if self.use_rsi and rsi_value is not None:
            if ma_signal == "BUY" and rsi_value >= self.rsi_buy_threshold:
                final_signal = "HOLD"
            elif ma_signal == "SELL" and rsi_value <= self.rsi_sell_threshold:
                final_signal = "HOLD"

        # Only output a new signal if not HOLD and different from the last signal.
        if final_signal != "HOLD" and final_signal != self.last_signal:
            self.last_signal = final_signal
            # Include both "action" and "signal" keys for RiskOps.
            result = {
                "symbol": self.symbol,
                "timestamp": timestamp,
                "price": price,
                "ma_short": round(ma_short_value, 2),
                "ma_long": round(ma_long_value, 2),
                "rsi": round(rsi_value, 2) if rsi_value is not None else None,
                "signal_type": final_signal,
                "action": final_signal,
                "signal": final_signal,
                "confidence": 1.0  # Default confidence for strategy-generated signals.
            }
        return result


def process_market_data(data: dict) -> dict:
    """
    Validate and (if needed) correct incoming market data from Dataflow.

    Expected keys:
      - symbol (str, required and non-empty)
      - price (int/float, required)
      - timestamp (str, optional; default: current UTC time)
      - volume (int/float, optional; default: 0)

    Logs warnings (in JSON format) if optional fields are missing or malformed.
    """
    # Validate required fields.
    if "symbol" not in data or not data.get("symbol") or not str(data["symbol"]).strip():
        err = {"error": "Missing or empty required field: 'symbol'"}
        log_json("ERROR", "Data validation error", extra=err)
        raise ValueError(err)
    if "price" not in data:
        err = {"error": "Missing required field: 'price'"}
        log_json("ERROR", "Data validation error", extra=err)
        raise ValueError(err)
    try:
        symbol = str(data["symbol"]).strip()
        price = float(data["price"])
    except (ValueError, TypeError) as e:
        log_json("ERROR", "Invalid type for 'symbol' or 'price'", extra={
            "exception": str(e),
            "stack_trace": traceback.format_exc()
        })
        raise

    # Process optional fields.
    timestamp = data.get("timestamp")
    if not timestamp:
        timestamp = datetime.utcnow().isoformat() + "Z"
        log_json("WARNING", "Missing timestamp; defaulting to current time", extra={"default_timestamp": timestamp})
    volume = data.get("volume")
    if volume is None:
        volume = 0
        log_json("WARNING", "Missing volume; defaulting to 0")
    else:
        try:
            volume = float(volume)
        except (ValueError, TypeError) as e:
            log_json("ERROR", "Invalid type for 'volume'", extra={
                "exception": str(e),
                "stack_trace": traceback.format_exc()
            })
            volume = 0

    processed_data = {
        "symbol": symbol,
        "timestamp": timestamp,
        "price": price,
        "volume": volume
    }
    log_json("INFO", "Processed market data", extra={"data": processed_data})
    return processed_data


def compute_signal(data: dict) -> dict:
    """
    Compute a trade signal using a simple strategy:
      - If price is below 125, signal is "BUY"; otherwise, "SELL".

    Returns a signal dict containing:
      symbol, timestamp, signal_type, action, signal, price, and confidence.
    Includes a retry mechanism and returns a fallback signal if needed.
    """
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            price = data["price"]
            signal_type = "BUY" if price < 125 else "SELL"
            signal = {
                "symbol": data["symbol"],
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "signal_type": signal_type,
                "action": signal_type,
                "signal": signal_type,
                "price": price,
                "confidence": round(random.uniform(0.7, 1.0), 2)
            }
            log_json("INFO", "Computed signal", extra={"signal": signal})
            return signal
        except KeyError as ke:
            log_json("ERROR", "Missing key during signal computation", extra={
                "exception": str(ke),
                "stack_trace": traceback.format_exc(),
                "attempt": attempt
            })
            break
        except Exception as e:
            log_json("ERROR", "Error computing signal", extra={
                "exception": str(e),
                "stack_trace": traceback.format_exc(),
                "attempt": attempt
            })
            time.sleep(0.5 * attempt)
    fallback_signal = {
        "symbol": data.get("symbol", "UNKNOWN"),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "signal_type": "HOLD",
        "action": "HOLD",
        "signal": "HOLD",
        "price": data.get("price", 0.0),
        "confidence": 0.0
    }
    log_json("INFO", "Returning fallback signal", extra={"signal": fallback_signal})
    return fallback_signal


def publish_trade_signal(signal: dict) -> None:
    """
    Publish the trade signal to the RabbitMQ 'trade_signals' queue.
    Implements exponential backoff for transient errors.

    The published message includes:
      - symbol, timestamp, signal_type, action, signal, price, confidence,
      - and optionally ma_short, ma_long, rsi.
    """
    rabbitmq_host = os.environ.get("RABBITMQ_HOST", "localhost")
    rabbitmq_user = os.environ.get("RABBITMQ_USER", "guest")
    rabbitmq_pass = os.environ.get("RABBITMQ_PASS", "guest")
    message = json.dumps(signal)
    max_retries = 3
    delay = 1

    for attempt in range(1, max_retries + 1):
        try:
            credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
            parameters = pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials)
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue="trade_signals", durable=True)
            channel.basic_publish(
                exchange="",
                routing_key="trade_signals",
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            log_json("INFO", "Published trade signal", extra={"signal": signal})
            connection.close()
            return
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
            log_json("ERROR", "RabbitMQ network error while publishing signal", extra={
                "exception": str(e),
                "stack_trace": traceback.format_exc(),
                "attempt": attempt
            })
        except Exception as e:
            log_json("ERROR", "Unexpected error while publishing trade signal", extra={
                "exception": str(e),
                "stack_trace": traceback.format_exc(),
                "attempt": attempt
            })
        time.sleep(delay)
        delay *= 2
    log_json("ERROR", "Failed to publish trade signal after retries", extra={"signal": signal})


# ---------------------------
# Optional Postgres Logging
# ---------------------------
class PostgresLogger:
    """
    Logs trading signals and performance metrics to the Postgres table "quant_signals".
    """
    def __init__(self, host: str, user: str, password: str, dbname: str) -> None:
        self.conn = psycopg2.connect(host=host, user=user, password=password, dbname=dbname)
        self.create_table()

    def create_table(self) -> None:
        create_query = """
        CREATE TABLE IF NOT EXISTS quant_signals (
            id SERIAL PRIMARY KEY,
            time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            symbol TEXT NOT NULL,
            action TEXT NOT NULL,
            ma_short NUMERIC,
            ma_long NUMERIC,
            rsi NUMERIC
        );
        """
        cur = self.conn.cursor()
        cur.execute(create_query)
        self.conn.commit()
        cur.close()

    def log_signal(self, signal: Dict[str, Any]) -> None:
        insert_query = """
        INSERT INTO quant_signals (time, symbol, action, ma_short, ma_long, rsi)
        VALUES (NOW(), %s, %s, %s, %s, %s);
        """
        cur = self.conn.cursor()
        cur.execute(insert_query, (
            signal.get("symbol"),
            signal.get("action"),
            signal.get("ma_short"),
            signal.get("ma_long"),
            signal.get("rsi")
        ))
        self.conn.commit()
        cur.close()

    def close(self) -> None:
        self.conn.close()


def get_rabbitmq_connection() -> pika.BlockingConnection:
    """
    Connect to RabbitMQ using environment variables.
    """
    rabbitmq_host = os.environ.get("RABBITMQ_HOST", "localhost")
    rabbitmq_user = os.environ.get("RABBITMQ_USER", "guest")
    rabbitmq_pass = os.environ.get("RABBITMQ_PASS", "guest")
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
    parameters = pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials)
    return pika.BlockingConnection(parameters)


# ---------------------------
# Main Consumer Logic
# ---------------------------
def main() -> None:
    log_json("INFO", "Starting Quant multi-symbol strategy...")
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue="market_data", durable=True)
    channel.queue_declare(queue="trade_signals", durable=True)

    postgres_logger: Optional[PostgresLogger] = None
    if DB_HOST and DB_USER and DB_PASS and DB_NAME:
        try:
            postgres_logger = PostgresLogger(DB_HOST, DB_USER, DB_PASS, DB_NAME)
            log_json("INFO", "Connected to Postgres for signal logging.")
        except Exception:
            log_json("ERROR", "Failed to connect to Postgres; continuing without DB logging", extra={"stack_trace": traceback.format_exc()})

    strategies: Dict[str, SymbolStrategy] = {}

    def on_message(ch, method, properties, body) -> None:
        try:
            raw_data = json.loads(body)
            processed_data = process_market_data(raw_data)
            symbol = processed_data["symbol"]
            timestamp = processed_data["timestamp"]
            price = processed_data["price"]

            if symbol not in strategies:
                strategies[symbol] = SymbolStrategy(
                    symbol=symbol,
                    ma_short=MA_SHORT,
                    ma_long=MA_LONG,
                    rsi_period=RSI_PERIOD,
                    use_rsi=USE_RSI_CONFIRMATION,
                    rsi_buy=RSI_BUY_THRESHOLD,
                    rsi_sell=RSI_SELL_THRESHOLD
                )
            signal = strategies[symbol].update(timestamp, price)
            if not signal:
                signal = compute_signal(processed_data)
            if signal and signal.get("signal_type") != "HOLD":
                publish_trade_signal(signal)
                log_json("INFO", "Published signal", extra={"signal": signal})
                if postgres_logger:
                    try:
                        postgres_logger.log_signal(signal)
                        log_json("DEBUG", "Signal logged to Postgres")
                    except Exception:
                        log_json("ERROR", "Failed to log signal to Postgres", extra={"stack_trace": traceback.format_exc()})
        except Exception:
            log_json("ERROR", "Error processing market data message", extra={"stack_trace": traceback.format_exc()})
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

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
        log_json("INFO", "Shutdown complete.")


# ---------------------------
# Unit Testing
# ---------------------------
import unittest

class TestQuantAnalysis(unittest.TestCase):
    def test_process_market_data_valid(self):
        data = {
            "symbol": "AAPL",
            "timestamp": "2025-02-07T12:00:00Z",
            "price": 120.5,
            "volume": 1000
        }
        processed = process_market_data(data)
        self.assertEqual(processed["symbol"], "AAPL")
        self.assertEqual(processed["price"], 120.5)
        self.assertEqual(processed["volume"], 1000)
        self.assertEqual(processed["timestamp"], "2025-02-07T12:00:00Z")

    def test_process_market_data_missing_optional(self):
        data = {
            "symbol": "GOOG",
            "price": 1500.0
        }
        processed = process_market_data(data)
        self.assertEqual(processed["symbol"], "GOOG")
        self.assertEqual(processed["price"], 1500.0)
        self.assertEqual(processed["volume"], 0)
        self.assertTrue("Z" in processed["timestamp"])  # Default timestamp provided.

    def test_process_market_data_missing_required(self):
        data = {
            "timestamp": "2025-02-07T12:00:00Z",
            "price": 100.0,
            "volume": 500
        }
        with self.assertRaises(ValueError):
            process_market_data(data)

    def test_process_market_data_empty_symbol(self):
        data = {
            "symbol": "   ",
            "price": 100.0,
            "volume": 500
        }
        with self.assertRaises(ValueError):
            process_market_data(data)

    def test_compute_signal_buy(self):
        data = {
            "symbol": "MSFT",
            "timestamp": "2025-02-07T12:00:00Z",
            "price": 120.0,
            "volume": 200
        }
        signal = compute_signal(data)
        self.assertEqual(signal["signal_type"], "BUY")
        self.assertEqual(signal["action"], "BUY")
        self.assertEqual(signal["signal"], "BUY")
        self.assertIn("confidence", signal)

    def test_compute_signal_sell(self):
        data = {
            "symbol": "IBM",
            "timestamp": "2025-02-07T12:00:00Z",
            "price": 130.0,
            "volume": 300
        }
        signal = compute_signal(data)
        self.assertEqual(signal["signal_type"], "SELL")
        self.assertEqual(signal["action"], "SELL")
        self.assertEqual(signal["signal"], "SELL")
        self.assertIn("confidence", signal)

    def test_symbol_strategy_update(self):
        strategy = SymbolStrategy("TEST", ma_short=2, ma_long=3, rsi_period=2,
                                    use_rsi=False, rsi_buy=30, rsi_sell=70)
        signals = []
        timestamps = [datetime.utcnow().isoformat() + "Z" for _ in range(3)]
        for price, ts in zip([100, 101, 102], timestamps):
            sig = strategy.update(ts, price)
            if sig:
                signals.append(sig)
        self.assertTrue(len(signals) >= 1)
        for sig in signals:
            self.assertIn("signal_type", sig)
            self.assertIn("action", sig)
            self.assertIn("signal", sig)
            self.assertIn("confidence", sig)

    def test_extreme_price_values(self):
        data = {
            "symbol": "EXTREME",
            "timestamp": "2025-02-07T12:00:00Z",
            "price": 1e9,  # Extremely high price.
            "volume": 1000
        }
        processed = process_market_data(data)
        signal = compute_signal(processed)
        self.assertEqual(signal["signal_type"], "SELL")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        sys.argv.pop(1)
        unittest.main()
    else:
        main()