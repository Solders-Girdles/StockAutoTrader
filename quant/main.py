#!/usr/bin/env python3
"""
quant/main.py

A robust demonstration of a moving average crossover strategy integrated with RabbitMQ.
This script consumes market data from the "market_data" queue, applies a simple
moving average crossover strategy (e.g., 2 vs. 5 MA), and publishes trading signals
(BUY/SELL) to the "trade_signals" queue.

Environment Variables:
    RABBITMQ_HOST: RabbitMQ host address (default: "localhost")
    RABBITMQ_USER: RabbitMQ username (default: "guest")
    RABBITMQ_PASS: RabbitMQ password (default: "guest")

(Optional for DB integration)
    DB_HOST, DB_USER, DB_PASS, DB_NAME
"""

import os
import json
import time
import logging
import pika
from collections import deque
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class SimpleMovingAverageCrossover:
    """
    A simple moving average crossover strategy that maintains a rolling
    buffer of prices, computes short and long moving averages, and generates
    BUY/SELL signals when a new data point arrives.
    """

    def __init__(self, short_window: int = 2, long_window: int = 5) -> None:
        """
        Initialize the strategy with the given window sizes.

        Args:
            short_window (int): Number of periods for the short moving average.
            long_window (int): Number of periods for the long moving average.
        """
        self.short_window = short_window
        self.long_window = long_window
        self.prices: deque[float] = deque(maxlen=long_window)
        self.last_action: str = ""  # To avoid repeating the same signal

    def on_new_price(self, symbol: str, timestamp: str, price: float) -> Dict[str, Any]:
        """
        Process a new price update, compute moving averages, and return
        a trading signal if the signal has changed.

        Args:
            symbol (str): The asset symbol (e.g., "AAPL").
            timestamp (str): The time of the price update.
            price (float): The new price value.

        Returns:
            Dict[str, Any]: A dictionary containing the signal data:
                {
                    "symbol": str,
                    "timestamp": str,
                    "price": float,
                    "action": str  # "BUY" or "SELL"
                }
            Returns an empty dict if not enough data is available or if the
            signal remains unchanged.
        """
        self.prices.append(price)
        logging.debug(f"Received price {price} for {symbol} at {timestamp}")

        # Ensure there is enough data to compute the moving averages.
        if len(self.prices) < self.long_window:
            logging.debug("Insufficient data for moving average calculation.")
            return {}

        # Compute the moving averages.
        short_prices = list(self.prices)[-self.short_window:]
        short_ma = sum(short_prices) / self.short_window
        long_ma = sum(self.prices) / self.long_window

        logging.debug(f"Short MA ({self.short_window}): {short_ma:.2f}")
        logging.debug(f"Long MA ({self.long_window}): {long_ma:.2f}")

        # Determine the signal: BUY if short MA > long MA, SELL if short MA < long MA.
        new_action = "BUY" if short_ma > long_ma else "SELL" if short_ma < long_ma else "HOLD"

        # Only generate a signal if it changes from the last action, and ignore "HOLD".
        if new_action == self.last_action or new_action == "HOLD":
            return {}

        self.last_action = new_action
        return {
            "symbol": symbol,
            "timestamp": timestamp,
            "price": price,
            "action": new_action
        }


def get_rabbitmq_connection() -> pika.BlockingConnection:
    """
    Establish a connection to RabbitMQ using environment variables.

    Returns:
        pika.BlockingConnection: The established RabbitMQ connection.
    """
    rabbitmq_host = os.environ.get("RABBITMQ_HOST", "localhost")
    rabbitmq_user = os.environ.get("RABBITMQ_USER", "guest")
    rabbitmq_pass = os.environ.get("RABBITMQ_PASS", "guest")

    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
    parameters = pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials)
    return pika.BlockingConnection(parameters)


def main() -> None:
    """
    Main loop for the moving average crossover strategy.
    Consumes market data from the "market_data" queue and publishes signals to "trade_signals".
    """
    logging.info("Starting Quant strategy...")

    # Connect to RabbitMQ.
    connection = get_rabbitmq_connection()
    channel = connection.channel()

    # Declare the queues.
    channel.queue_declare(queue="market_data", durable=True)
    channel.queue_declare(queue="trade_signals", durable=True)

    # Initialize the strategy instance.
    strategy = SimpleMovingAverageCrossover(short_window=2, long_window=5)

    def on_message(ch, method, properties, body) -> None:
        """
        Callback for processing incoming market data messages.
        """
        try:
            data = json.loads(body)
            symbol = data.get("symbol", "UNKNOWN")
            timestamp = data.get("timestamp", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
            price = data.get("price")
            if price is None:
                logging.warning("Received data without a price, skipping.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            logging.debug(f"Processing data: {data}")
            signal = strategy.on_new_price(symbol, timestamp, price)

            if signal:
                signal_message = json.dumps(signal)
                channel.basic_publish(
                    exchange="",
                    routing_key="trade_signals",
                    body=signal_message,
                    properties=pika.BasicProperties(delivery_mode=2)  # make message persistent
                )
                logging.info(f"Published signal: {signal_message}")
        except Exception as e:
            logging.exception("Error processing market data message")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    # Start consuming from the "market_data" queue.
    channel.basic_consume(queue="market_data", on_message_callback=on_message)
    logging.info("Waiting for market data. To exit press CTRL+C")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("Interrupted by user, stopping...")
    finally:
        channel.stop_consuming()
        connection.close()
        logging.info("RabbitMQ connection closed.")


if __name__ == "__main__":
    main()