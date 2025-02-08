#!/usr/bin/env python3
"""
dataflow/main.py

A production-ready real-time data ingestion script for the StockAutoTrader project.

Features:
  - Connects to RabbitMQ using credentials from environment variables.
  - Publishes real-time data events (symbol, price, timestamp) to the "market_data" queue.
  - Optionally logs ingestion events to a Postgres database if credentials are provided.
  - Runs continuously, generating and publishing new data every few seconds.

Environment Variables:
  - RabbitMQ: RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS
  - (Optional) Postgres: DB_HOST, DB_USER, DB_PASS, DB_NAME
Usage:
  Ensure the required environment variables are set (e.g., in your Docker container) and run:
      python dataflow/main.py
"""

import os
import time
import random
import json
import datetime
import logging

import pika
import psycopg2

# Configure logging for production diagnostics.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("DataFlow")

# --- RabbitMQ Setup ---
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")

credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
try:
    rabbit_connection = pika.BlockingConnection(parameters)
    channel = rabbit_connection.channel()
    channel.queue_declare(queue="market_data", durable=True)
    logger.info("Connected to RabbitMQ at %s", RABBITMQ_HOST)
except Exception as e:
    logger.error("Failed to connect to RabbitMQ: %s", e)
    raise

# --- Optional Postgres Setup ---
DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
db_conn = None

if DB_HOST and DB_USER and DB_PASS and DB_NAME:
    try:
        db_conn = psycopg2.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASS, dbname=DB_NAME
        )
        db_conn.autocommit = True
        with db_conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_log (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10),
                    price NUMERIC,
                    timestamp TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
        logger.info("Connected to Postgres at %s", DB_HOST)
    except Exception as e:
        logger.error("Failed to connect to Postgres: %s", e)
        db_conn = None


def generate_mock_data(symbol: str) -> dict:
    """
    Generates a mock data event for the given symbol.

    Parameters:
        symbol (str): The stock symbol for which to generate data.

    Returns:
        dict: A dictionary containing the symbol, a random price, and the current UTC timestamp.
    """
    return {
        "symbol": symbol,
        "price": round(random.uniform(100, 200), 2),
        "timestamp": datetime.datetime.utcnow().isoformat()
    }


def publish_data_event(data: dict) -> None:
    """
    Publishes the given data event to the RabbitMQ 'market_data' queue.

    Parameters:
        data (dict): The data event to publish.
    """
    message = json.dumps(data)
    channel.basic_publish(
        exchange="",
        routing_key="market_data",
        body=message,
        properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent.
    )
    logger.info("Published data event: %s", message)


def log_to_postgres(data: dict) -> None:
    """
    Logs the given data event to Postgres (if a connection is available).

    Parameters:
        data (dict): The data event to log.
    """
    if db_conn:
        try:
            with db_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ingestion_log (symbol, price, timestamp)
                    VALUES (%s, %s, %s)
                    """,
                    (data["symbol"], data["price"], data["timestamp"])
                )
            logger.debug("Logged data event to Postgres: %s", data)
        except Exception as e:
            logger.error("Failed to log data event to Postgres: %s", e)


def main() -> None:
    """
    Main loop: continuously generate and publish data events.
    """
    symbols = ["AAPL", "MSFT", "GOOG"]  # Example list of symbols.
    logger.info("Starting real-time data ingestion for symbols: %s", ", ".join(symbols))
    logger.info("Press Ctrl+C to exit.\n")

    try:
        while True:
            for symbol in symbols:
                data_event = generate_mock_data(symbol)
                publish_data_event(data_event)
                log_to_postgres(data_event)
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Ingestion loop interrupted by user (KeyboardInterrupt).")
    except Exception as e:
        logger.error("Unexpected error in main loop: %s", e)
    finally:
        if rabbit_connection and not rabbit_connection.is_closed:
            rabbit_connection.close()
        if db_conn:
            db_conn.close()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    main()