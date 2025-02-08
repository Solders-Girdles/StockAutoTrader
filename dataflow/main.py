#!/usr/bin/env python3
"""
dataflow/main.py

A production-ready real-time market data ingestion script for the StockAutoTrader project.

Features:
  - Integrates with the Polygon API to fetch market data for multiple symbols.
  - Publishes data events (symbol, price, volume, timestamp) to RabbitMQ.
  - Implements retry logic with exponential backoff for transient errors.
  - Uses structured JSON logging for standardized output.
  - Exposes Prometheus metrics via an HTTP endpoint for monitoring and load testing.
  - Organized into modular classes (PolygonService, RabbitMQPublisher).
  - Includes basic unit tests to mock Polygon API calls and RabbitMQ publishing.

Environment Variables:
  - POLYGON_API_KEY: API key for Polygon.
  - RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS: RabbitMQ connection parameters.
  - WATCHLIST: Comma-separated list of symbols (default: "AAPL,MSFT,GOOG,TSLA").

Usage:
  To run the service:
      python dataflow/main.py

  To run unit tests:
      python dataflow/main.py test
"""

import os
import time
import json
import datetime
import logging
import random
import sys

import requests
import pika
from pythonjsonlogger import jsonlogger

# Prometheus client imports
from prometheus_client import start_http_server, Counter, Histogram

# For unit testing
import unittest
from unittest.mock import patch, MagicMock

# --- Structured JSON Logging Configuration ---
logger = logging.getLogger("DataFlow")
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    fmt='%(asctime)s %(name)s %(levelname)s %(message)s',
    rename_fields={
        'asctime': 'timestamp',
        'name': 'service',
        'levelname': 'level',
        'message': 'message'
    }
)
stream_handler.setFormatter(formatter)
logger.handlers = []
logger.addHandler(stream_handler)

# --- Environment Variables & Configuration ---
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")
WATCHLIST = os.environ.get("WATCHLIST", "AAPL,MSFT,GOOG,TSLA").split(",")

# Retry configuration for Polygon API calls
MAX_RETRIES = 5
INITIAL_BACKOFF = 1  # in seconds

# --- Prometheus Metrics ---
polygon_api_calls = Counter(
    'polygon_api_calls_total',
    'Total number of calls made to Polygon API',
    ['symbol', 'status']  # status: success or failure
)
polygon_api_response_time = Histogram(
    'polygon_api_response_time_seconds',
    'Response time for Polygon API calls in seconds',
    ['symbol']
)
published_messages = Counter(
    'published_messages_total',
    'Total number of messages published to RabbitMQ'
)
publishing_errors = Counter(
    'publishing_errors_total',
    'Total number of publishing errors to RabbitMQ'
)


# --- Polygon API Integration ---
class PolygonService:
    """
    Service class to interact with the Polygon API.
    """

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("POLYGON_API_KEY is not set.")
        self.api_key = api_key
        self.base_url = "https://api.polygon.io/v2/last/trade"

    def fetch_market_data(self, symbol: str) -> dict:
        """
        Fetch market data for a given symbol from Polygon API with retry logic.

        Args:
            symbol (str): The ticker symbol to fetch.

        Returns:
            dict: Contains keys 'symbol', 'price', 'volume', 'timestamp'.

        On persistent failure, falls back to generated mock data.
        """
        url = f"{self.base_url}/{symbol}"
        params = {"apiKey": self.api_key}
        retries = 0
        backoff = INITIAL_BACKOFF

        while retries < MAX_RETRIES:
            try:
                logger.info("Fetching market data for %s", symbol)
                with polygon_api_response_time.labels(symbol=symbol).time():
                    response = requests.get(url, params=params, timeout=5)
                if response.status_code == 429:
                    logger.warning("Rate limit reached for %s. Retrying after backoff.", symbol)
                    polygon_api_calls.labels(symbol=symbol, status='failure').inc()
                    raise Exception("Rate limited")
                response.raise_for_status()
                data = response.json()
                last_trade = data.get("last", {})
                timestamp_ms = last_trade.get("timestamp")
                if timestamp_ms is None:
                    logger.warning("Timestamp missing in API response for %s. Using current time.", symbol)
                    timestamp = datetime.datetime.utcnow().isoformat()
                else:
                    timestamp = datetime.datetime.utcfromtimestamp(timestamp_ms / 1000.0).isoformat()
                price = last_trade.get("price")
                if price is None:
                    raise ValueError("Missing price in API response")
                volume = last_trade.get("size", 0)
                polygon_api_calls.labels(symbol=symbol, status='success').inc()
                return {
                    "symbol": symbol,
                    "price": price,
                    "volume": volume,
                    "timestamp": timestamp,
                }
            except Exception as e:
                retries += 1
                logger.error("Error fetching data for %s (attempt %s/%s): %s", symbol, retries, MAX_RETRIES, e)
                time.sleep(backoff)
                backoff *= 2  # Exponential backoff

        logger.error("Failed to fetch data for %s after %s attempts. Falling back to mock data.", symbol, MAX_RETRIES)
        polygon_api_calls.labels(symbol=symbol, status='failure').inc()
        return self.generate_mock_data(symbol)

    def generate_mock_data(self, symbol: str) -> dict:
        """
        Generate fallback mock market data for a given symbol.
        """
        now_utc = datetime.datetime.utcnow().isoformat()
        mock_price = round(random.uniform(100, 200), 2)
        mock_volume = random.randint(100, 1000)
        return {
            "symbol": symbol,
            "price": mock_price,
            "volume": mock_volume,
            "timestamp": now_utc,
        }


# --- RabbitMQ Publishing ---
class RabbitMQPublisher:
    """
    Publisher class to send messages to RabbitMQ.
    """

    def __init__(self, host: str, user: str, password: str, queue: str = "market_data"):
        self.host = host
        self.user = user
        self.password = password
        self.queue = queue
        self.connection = None
        self.channel = None
        self.connect()

    def connect(self):
        """
        Establish connection to RabbitMQ and declare the queue.
        """
        try:
            credentials = pika.PlainCredentials(self.user, self.password)
            parameters = pika.ConnectionParameters(host=self.host, credentials=credentials)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue, durable=True)
            logger.info("Connected to RabbitMQ at %s", self.host)
        except Exception as e:
            logger.error("Failed to connect to RabbitMQ: %s", e)
            raise

    def publish(self, message: dict):
        """
        Publish a message (dict) as JSON to the configured RabbitMQ queue.
        """
        try:
            json_message = json.dumps(message)
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue,
                body=json_message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            published_messages.inc()
            logger.info("Published message to RabbitMQ: %s", json_message)
        except Exception as e:
            publishing_errors.inc()
            logger.error("Failed to publish message to RabbitMQ: %s", e)
            raise

    def close(self):
        """
        Close the RabbitMQ connection.
        """
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("RabbitMQ connection closed.")


# --- Main Application Loop ---
def main():
    """
    Main loop: periodically fetch market data from Polygon and publish to RabbitMQ.
    Also starts an HTTP server to expose Prometheus metrics.
    """
    # Start Prometheus metrics server on port 8000
    start_http_server(8000)
    logger.info("Started Prometheus metrics server on port 8000")

    polygon_service = PolygonService(api_key=POLYGON_API_KEY)
    publisher = RabbitMQPublisher(
        host=RABBITMQ_HOST,
        user=RABBITMQ_USER,
        password=RABBITMQ_PASS,
        queue="market_data"
    )

    try:
        while True:
            for symbol in WATCHLIST:
                data_event = polygon_service.fetch_market_data(symbol)
                # Validate required fields
                if "symbol" not in data_event:
                    logger.warning("Missing 'symbol' in data event, skipping publish.")
                    continue
                if "price" not in data_event:
                    logger.warning("Missing 'price' in data event for %s, setting default 0.0.", symbol)
                    data_event["price"] = 0.0
                if "volume" not in data_event:
                    logger.warning("Missing 'volume' in data event for %s, setting default 0.", symbol)
                    data_event["volume"] = 0
                if "timestamp" not in data_event:
                    logger.warning("Missing 'timestamp' in data event for %s, using current UTC time.", symbol)
                    data_event["timestamp"] = datetime.datetime.utcnow().isoformat()

                publisher.publish(data_event)
            # Throttle cycles (e.g., every 5 seconds)
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Shutting down.")
    except Exception as e:
        logger.error("Unexpected error in main loop: %s", e, exc_info=True)
    finally:
        publisher.close()


# --- Basic Unit Tests ---
class TestPolygonService(unittest.TestCase):
    @patch("requests.get")
    def test_fetch_market_data_success(self, mock_get):
        # Mock a successful Polygon API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "last": {
                "timestamp": 1609459200000,  # Corresponds to 2021-01-01T00:00:00Z
                "price": 150.0,
                "size": 500
            }
        }
        mock_get.return_value = mock_response

        service = PolygonService(api_key="dummy")
        result = service.fetch_market_data("AAPL")
        self.assertEqual(result["symbol"], "AAPL")
        self.assertEqual(result["price"], 150.0)
        self.assertEqual(result["volume"], 500)
        datetime.datetime.fromisoformat(result["timestamp"])

    @patch("requests.get")
    def test_fetch_market_data_rate_limit(self, mock_get):
        # Simulate a rate limit response (HTTP 429)
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_get.return_value = mock_response

        service = PolygonService(api_key="dummy")
        result = service.fetch_market_data("AAPL")
        self.assertEqual(result["symbol"], "AAPL")
        self.assertTrue(100 <= result["price"] <= 200)


class TestRabbitMQPublisher(unittest.TestCase):
    @patch("pika.BlockingConnection")
    def test_publish(self, mock_connection):
        fake_channel = MagicMock()
        fake_connection = MagicMock()
        fake_connection.channel.return_value = fake_channel
        mock_connection.return_value = fake_connection

        publisher = RabbitMQPublisher(
            host="localhost",
            user="guest",
            password="guest",
            queue="market_data"
        )
        message = {"symbol": "AAPL", "price": 150.0, "volume": 500, "timestamp": "2021-01-01T00:00:00"}
        publisher.publish(message)
        fake_channel.basic_publish.assert_called_once()
        publisher.close()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        sys.argv.pop(1)
        unittest.main()
    else:
        main()