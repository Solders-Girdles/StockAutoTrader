#!/usr/bin/env python3
"""
quant/main.py

A robust multi-symbol moving average crossover strategy integrated with RabbitMQ and optional Postgres logging.
This script consumes market data from the "market_data" queue, applies configurable strategy logic
(including MA crossover and optional RSI confirmation), and publishes trading signals to the "trade_signals" queue.
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
import pika
import psycopg2
from collections import deque
from typing import Dict, Any, Deque, Optional

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
            data = json.loads(body)
            symbol = data.get("symbol", "UNKNOWN")
            timestamp = data.get("timestamp", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
            price = data.get("price")
            if price is None:
                logging.warning("Received market data without a price; skipping message.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            # Create a new strategy instance for the symbol if needed.
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
            strategy = strategies[symbol]
            signal = strategy.update(timestamp, price)

            if signal:
                # Publish the signal to the "trade_signals" queue.
                signal_message = json.dumps(signal)
                channel.basic_publish(
                    exchange="",
                    routing_key="trade_signals",
                    body=signal_message,
                    properties=pika.BasicProperties(delivery_mode=2)  # persistent message
                )
                logging.info(f"Published signal: {signal_message}")
                # Optionally log the signal to Postgres.
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