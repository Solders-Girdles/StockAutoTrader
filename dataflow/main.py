#!/usr/bin/env python3
"""
dataflow/main.py

A production-ready real-time market data ingestion script for the StockAutoTrader project.

Features:
  - Fetches real-time price data from a live API (Polygon.io example) for multiple symbols.
  - Implements a fallback mechanism using mock data if the API call fails or is rate-limited.
  - Publishes data events (symbol, price, volume, timestamp) to the RabbitMQ "market_data" queue.
  - Optionally logs ingestion events to a Postgres database for analytics.
  - Reads a watchlist from the WATCHLIST environment variable (a comma-separated list of symbols).

Environment Variables:
  - API: POLYGON_API_KEY  (for real data source)
  - RabbitMQ: RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS
  - Postgres (optional): DB_HOST, DB_USER, DB_PASS, DB_NAME
  - Watchlist: WATCHLIST (e.g., "AAPL,MSFT,GOOG,TSLA")

Usage:
  Ensure all required environment variables are set in your Docker container and run:
      python dataflow/main.py
"""

import os
import time
import random
import json
import datetime
import logging

import requests
import pika
import psycopg2

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("DataFlow")

# --- Environment Variables ---
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")  # For real data

# RabbitMQ configuration
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")

# Postgres configuration (optional)
DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")

# Watchlist: comma-separated symbols; defaults if not set.
WATCHLIST = os.environ.get("WATCHLIST", "AAPL,MSFT,GOOG,TSLA").split(",")

# --- RabbitMQ Connection ---
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

# --- Optional Postgres Connection ---
db_conn = None
if DB_HOST and DB_USER and DB_PASS and DB_NAME:
    try:
        db_conn = psycopg2.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASS, dbname=DB_NAME
        )
        db_conn.autocommit = True
        with db_conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_logs (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10),
                    price NUMERIC,
                    volume NUMERIC,
                    timestamp TIMESTAMP,
                    fetched_at TIMESTAMP DEFAULT NOW()
                )
            """)
        logger.info("Connected to Postgres at %s", DB_HOST)
    except Exception as e:
        logger.error("Failed to connect to Postgres: %s", e)
        db_conn = None


def fetch_real_data(symbol: str) -> dict:
    """
    Fetch real-time data for a symbol using the Polygon.io API.
    If the API call fails (e.g., due to rate limiting or other errors), an exception is raised.
    """
    if not POLYGON_API_KEY:
        raise ValueError("POLYGON_API_KEY is not set. Cannot fetch real data.")

    url = f"https://api.polygon.io/v2/last/trade/{symbol}"
    params = {"apiKey": POLYGON_API_KEY}
    logger.debug("Fetching real data for %s from Polygon.io", symbol)
    try:
        response = requests.get(url, params=params, timeout=5)
        if response.status_code == 429:
            logger.warning("Rate limit reached for %s. Using fallback mock data.", symbol)
            raise Exception("Rate limited")
        response.raise_for_status()
        data = response.json()
        last_trade = data.get("last", {})
        timestamp_ms = last_trade.get("timestamp")
        if timestamp_ms is None:
            raise ValueError(f"Missing timestamp in API response for {symbol}")
        dt_utc = datetime.datetime.utcfromtimestamp(timestamp_ms / 1000.0).isoformat()
        price = last_trade.get("price")
        volume = last_trade.get("size", 0)
        if price is None:
            raise ValueError(f"Missing price in API response for {symbol}")
        return {
            "symbol": symbol,
            "price": price,
            "volume": volume,
            "timestamp": dt_utc,
        }
    except Exception as e:
        logger.error("Error fetching real data for %s: %s", symbol, e)
        raise


def generate_mock_data(symbol: str) -> dict:
    """
    Generate mock market data for a given symbol.
    """
    now_utc = datetime.datetime.utcnow().isoformat()
    mock_price = round(random.uniform(100, 200), 2)
    mock_volume = random.randint(100, 1000)
    logger.debug("Generated mock data for %s", symbol)
    return {
        "symbol": symbol,
        "price": mock_price,
        "volume": mock_volume,
        "timestamp": now_utc,
    }


def get_market_data(symbol: str) -> dict:
    """
    Attempts to fetch real data for a symbol. On failure, falls back to generating mock data.
    """
    try:
        return fetch_real_data(symbol)
    except Exception as e:
        logger.info("Falling back to mock data for %s due to error: %s", symbol, e)
        return generate_mock_data(symbol)


def publish_data_event(data: dict) -> None:
    """
    Publishes the given market data event to the RabbitMQ "market_data" queue.
    """
    message = json.dumps(data)
    try:
        channel.basic_publish(
            exchange="",
            routing_key="market_data",
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)  # Persistent message.
        )
        logger.info("Published data event: %s", message)
    except Exception as e:
        logger.error("Failed to publish data event: %s", e)


def log_to_postgres(data: dict) -> None:
    """
    Logs the market data event to the Postgres database (if a connection is available).
    """
    if db_conn:
        try:
            with db_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ingestion_logs (symbol, price, volume, timestamp)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (data["symbol"], data["price"], data["volume"], data["timestamp"])
                )
            logger.debug("Logged data event to Postgres: %s", data)
        except Exception as e:
            logger.error("Error logging data event to Postgres: %s", e)


def main() -> None:
    """
    Main loop: continuously fetch market data for each symbol in the watchlist,
    publish to RabbitMQ, and log ingestion events to Postgres if configured.
    """
    logger.info("Starting market data ingestion for watchlist: %s", WATCHLIST)
    logger.info("Press Ctrl+C to exit.")

    try:
        while True:
            for symbol in WATCHLIST:
                data_event = get_market_data(symbol)
                publish_data_event(data_event)
                log_to_postgres(data_event)
            # Sleep between cycles to throttle API calls and publishing.
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down.")
    except Exception as e:
        logger.error("Unexpected error in main loop: %s", e, exc_info=True)
    finally:
        if rabbit_connection and not rabbit_connection.is_closed:
            rabbit_connection.close()
        if db_conn:
            db_conn.close()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    main()