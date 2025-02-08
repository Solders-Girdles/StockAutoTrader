#!/usr/bin/env python3
"""
quant/main.py

A robust multi-symbol moving average crossover strategy integrated with RabbitMQ,
enhanced error handling, and optional Postgres logging.
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

import pika
import psycopg2

# Configure logging.
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

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


class SymbolStrategy:
    """
    Strategy for a single symbol using configurable moving averages and RSI indicator.
    Maintains a rolling window of prices and computes:
      - A short MA over the last MA_SHORT prices.
      - A long MA over the last MA_LONG prices.
      - An RSI over the last RSI_PERIOD price changes (if enough data).

    The strategy generates a BUY or SELL signal when the short MA crosses over the long MA,
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
        # Store enough prices for both MA and RSI calculations.
        self.buffer_size = max(ma_long, rsi_period + 1)
        self.prices: Deque[float] = deque(maxlen=self.buffer_size)
        self.last_signal: str = ""

    def update(self, timestamp: str, price: float) -> Dict[str, Any]:
        """
        Update the strategy with a new price, compute indicators, and determine if a new signal is generated.

        Args:
            timestamp (str): The timestamp of the data point.
            price (float): The latest price.

        Returns:
            Dict[str, Any]: A dict containing the signal and indicator values, or an empty dict if no new signal.
        """
        self.prices.append(price)
        result: Dict[str, Any] = {}
        if len(self.prices) < self.ma_long:
            # Not enough data to compute moving averages.
            return result

        prices_list = list(self.prices)
        short_prices = prices_list[-self.ma_short:]
        long_prices = prices_list[-self.ma_long:]
        ma_short_value = sum(short_prices) / self.ma_short
        ma_long_value = sum(long_prices) / self.ma_long

        # Determine MA-based signal.
        if ma_short_value > ma_long_value:
            ma_signal = "BUY"
        elif ma_short_value < ma_long_value:
            ma_signal = "SELL"
        else:
            ma_signal = "HOLD"

        # Compute RSI if enough data is available.
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

        # Combine MA and RSI signals.
        final_signal = ma_signal
        if self.use_rsi and rsi_value is not None:
            # For a BUY signal, require RSI to be below the buy threshold.
            # For a SELL signal, require RSI to be above the sell threshold.
            if ma_signal == "BUY" and rsi_value >= self.rsi_buy_threshold:
                final_signal = "HOLD"
            elif ma_signal == "SELL" and rsi_value <= self.rsi_sell_threshold:
                final_signal = "HOLD"

        # Only output a signal if it is not HOLD and has changed.
        if final_signal != "HOLD" and final_signal != self.last_signal:
            self.last_signal = final_signal
            result = {
                "symbol": self.symbol,
                "timestamp": timestamp,
                "price": price,
                "ma_short": round(ma_short_value, 2),
                "ma_long": round(ma_long_value, 2),
                "rsi": round(rsi_value, 2) if rsi_value is not None else None,
                "action": final_signal
            }
        return result


class PostgresLogger:
    """
    Optionally logs trading signals and performance metrics to a Postgres table "quant_signals".
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
    Connects to RabbitMQ using environment variables.

    Returns:
        pika.BlockingConnection: The established connection.
    """
    rabbitmq_host = os.environ.get("RABBITMQ_HOST", "localhost")
    rabbitmq_user = os.environ.get("RABBITMQ_USER", "guest")
    rabbitmq_pass = os.environ.get("RABBITMQ_PASS", "guest")
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
    parameters = pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials)
    return pika.BlockingConnection(parameters)


def process_market_data(data: dict) -> dict:
    """
    Process and validate incoming market data.

    The market data dictionary must contain the following keys:
      - symbol: str
      - timestamp: str
      - price: (int or float)
      - volume: (int or float)

    This function logs the received data and returns a validated dictionary.
    Detailed error information (including stack traces) is logged as JSON.

    Args:
        data (dict): Incoming market data.

    Returns:
        dict: Processed market data.
    """
    required_keys = ["symbol", "timestamp", "price", "volume"]
    for key in required_keys:
        if key not in data:
            error_msg = {"error": f"Missing required market data key: '{key}'"}
            logging.error(json.dumps(error_msg))
            raise ValueError(error_msg)
    try:
        symbol = str(data["symbol"])
        timestamp = str(data["timestamp"])
        price = float(data["price"])
        volume = float(data["volume"])
    except ValueError as ve:
        logging.error(
            json.dumps({
                "error": "Invalid data type in market data",
                "exception": str(ve),
                "stack_trace": traceback.format_exc()
            })
        )
        raise ValueError(f"Invalid data type in market data: {ve}")
    except TypeError as te:
        logging.error(
            json.dumps({
                "error": "Invalid data type in market data",
                "exception": str(te),
                "stack_trace": traceback.format_exc()
            })
        )
        raise ValueError(f"Invalid data type in market data: {te}")

    processed_data = {
        "symbol": symbol,
        "timestamp": timestamp,
        "price": price,
        "volume": volume
    }
    logging.info(json.dumps({"message": "Processed market data", "data": processed_data}))
    return processed_data


def compute_signal(data: dict) -> dict:
    """
    Compute a trade signal from processed market data using a simple strategy.

    Strategy:
      - If the price is below 125, signal_type is "BUY".
      - Otherwise, signal_type is "SELL".

    The generated signal dictionary contains:
      - symbol: str (from market data)
      - timestamp: str (current UTC time in ISO8601 format)
      - signal_type: str ("BUY" or "SELL")
      - price: float (from market data)
      - confidence: float (random value between 0.7 and 1.0)

    Robust error handling with a retry mechanism is applied.
    Detailed error information is logged in JSON format.

    Args:
        data (dict): Processed market data.

    Returns:
        dict: Trade signal.
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
                "price": price,
                "confidence": round(random.uniform(0.7, 1.0), 2)
            }
            logging.info(json.dumps({"message": "Computed signal successfully", "signal": signal}))
            return signal
        except KeyError as ke:
            logging.error(
                json.dumps({
                    "error": "Missing key in market data during signal computation",
                    "exception": str(ke),
                    "stack_trace": traceback.format_exc(),
                    "attempt": attempt
                })
            )
            break  # No point retrying if a required key is missing.
        except Exception as e:
            logging.error(
                json.dumps({
                    "error": "Error computing signal",
                    "exception": str(e),
                    "stack_trace": traceback.format_exc(),
                    "attempt": attempt
                })
            )
            time.sleep(0.5 * attempt)  # Exponential backoff

    # Fallback: return a default signal indicating no actionable decision.
    fallback_signal = {
        "symbol": data.get("symbol", "UNKNOWN"),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "signal_type": "HOLD",
        "price": data.get("price", 0.0),
        "confidence": 0.0
    }
    logging.info(json.dumps({"message": "Returning fallback signal", "fallback_signal": fallback_signal}))
    return fallback_signal


def publish_trade_signal(signal: dict) -> None:
    """
    Publish the trade signal to the RabbitMQ 'trade_signals' queue.

    The function reads the RabbitMQ connection details from environment variables:
      - RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS

    It encodes the signal as JSON and publishes it with persistent delivery mode.
    Network-related errors are caught with specific exception handling, and a simple
    retry logic with exponential backoff is applied.

    Args:
        signal (dict): The trade signal to publish.
    """
    rabbitmq_host = os.environ.get("RABBITMQ_HOST", "localhost")
    rabbitmq_user = os.environ.get("RABBITMQ_USER", "guest")
    rabbitmq_pass = os.environ.get("RABBITMQ_PASS", "guest")
    message = json.dumps(signal)

    max_retries = 3
    delay = 1  # Initial delay in seconds

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
                properties=pika.BasicProperties(delivery_mode=2)  # persistent message
            )
            logging.info(json.dumps({"message": "Successfully published trade signal", "signal": signal}))
            connection.close()
            return
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
            logging.error(
                json.dumps({
                    "error": "RabbitMQ network-related error while publishing trade signal",
                    "exception": str(e),
                    "stack_trace": traceback.format_exc(),
                    "attempt": attempt
                })
            )
        except Exception as e:
            logging.error(
                json.dumps({
                    "error": "Unexpected error while publishing trade signal",
                    "exception": str(e),
                    "stack_trace": traceback.format_exc(),
                    "attempt": attempt
                })
            )
        time.sleep(delay)
        delay *= 2  # Exponential backoff

    # If all attempts fail, log the final error.
    logging.error(json.dumps({"error": "Failed to publish trade signal after multiple attempts", "signal": signal}))


def main() -> None:
    logging.info("Starting Quant multi-symbol strategy...")

    # Establish connection to RabbitMQ.
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue="market_data", durable=True)
    channel.queue_declare(queue="trade_signals", durable=True)

    # Initialize Postgres logger if DB parameters are provided.
    postgres_logger: Optional[PostgresLogger] = None
    if DB_HOST and DB_USER and DB_PASS and DB_NAME:
        try:
            postgres_logger = PostgresLogger(DB_HOST, DB_USER, DB_PASS, DB_NAME)
            logging.info("Connected to Postgres for signal logging.")
        except Exception:
            logging.exception("Failed to connect to Postgres. Proceeding without DB logging.")

    # Dictionary to hold strategy instances per symbol.
    strategies: Dict[str, SymbolStrategy] = {}

    def on_message(ch, method, properties, body) -> None:
        try:
            raw_data = json.loads(body)
            # Validate and process market data.
            processed_data = process_market_data(raw_data)
            symbol = processed_data["symbol"]
            timestamp = processed_data["timestamp"]
            price = processed_data["price"]

            # Use the multi-symbol strategy.
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
            # If no signal from the strategy, use the fallback compute_signal.
            if not signal:
                signal = compute_signal(processed_data)
            # Only publish if there is an actionable signal.
            if signal and signal.get("signal_type") != "HOLD":
                publish_trade_signal(signal)
                logging.info(f"Published signal: {json.dumps(signal)}")
                if postgres_logger:
                    try:
                        postgres_logger.log_signal(signal)
                        logging.debug("Signal logged to Postgres.")
                    except Exception:
                        logging.exception("Failed to log signal to Postgres.")
        except Exception:
            logging.exception("Error processing market data message.")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue="market_data", on_message_callback=on_message)
    logging.info("Waiting for market data. To exit press CTRL+C")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("Interrupted by user. Shutting down...")
    finally:
        channel.stop_consuming()
        connection.close()
        if postgres_logger:
            postgres_logger.close()
        logging.info("Shutdown complete.")


if __name__ == "__main__":
    main()