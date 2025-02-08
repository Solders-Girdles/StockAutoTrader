#!/usr/bin/env python3
"""
dataflow/main.py

A production-ready real-time market data ingestion script for the StockAutoTrader project.

Features:
  - Integrates with the primary Polygon API to fetch market data for multiple symbols.
  - Implements a secondary data source (SecondaryPolygonService) as a backup feed.
  - If the primary feed becomes unavailable or hits rate limits, the service automatically fails over to the secondary feed.
  - Implements strict schema validation for incoming market data.
  - If data is incomplete or malformed, logs a warning and marks the data as partial.
  - Publishes data events (with an added correlation/trace ID) to RabbitMQ.
  - Uses retry logic with exponential backoff for transient errors.
  - Exposes Prometheus metrics (e.g., API call latency, published messages, failovers) via an HTTP endpoint for monitoring.
  - Structured JSON logging is used throughout with keys: timestamp, service, level, message (plus trace_id for published messages).

Environment Variables:
  - POLYGON_API_KEY: API key for the primary Polygon feed.
  - SECONDARY_POLYGON_BASE_URL: Base URL for the secondary data feed (default: "https://secondary-api.polygon.io/v2/last/trade").
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
import uuid

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
SECONDARY_POLYGON_BASE_URL = os.environ.get("SECONDARY_POLYGON_BASE_URL", "https://secondary-api.polygon.io/v2/last/trade")
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")
WATCHLIST = os.environ.get("WATCHLIST", "AAPL,MSFT,GOOG,TSLA").split(",")

# Retry configuration for API calls
MAX_RETRIES = 5
INITIAL_BACKOFF = 1  # in seconds

# --- Prometheus Metrics ---
polygon_api_calls = Counter(
    'polygon_api_calls_total',
    'Total number of calls made to the primary Polygon API',
    ['symbol', 'status']  # status: success or failure
)
secondary_api_calls = Counter(
    'secondary_api_calls_total',
    'Total number of calls made to the secondary data feed',
    ['symbol', 'status']
)
polygon_api_response_time = Histogram(
    'polygon_api_response_time_seconds',
    'Response time for primary Polygon API calls in seconds',
    ['symbol']
)
secondary_api_response_time = Histogram(
    'secondary_api_response_time_seconds',
    'Response time for secondary API calls in seconds',
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
secondary_failovers = Counter(
    'secondary_failovers_total',
    'Total number of times failover to secondary data feed occurred'
)

# --- Schema Validation Function ---
def validate_market_data(data: dict) -> dict:
    """
    Validate the market data dictionary to ensure required fields are present.
    If a field is missing or malformed, log a warning and mark the data as partial.

    Required fields:
      - symbol: string
      - price: float (or int)
      - volume: float or int
      - timestamp: ISO8601 string

    Returns:
        dict: The validated data, possibly with an added "is_partial": True flag.
              Returns None if critical fields (e.g., symbol) are missing.
    """
    is_partial = False

    # Validate 'symbol'
    if "symbol" not in data or not isinstance(data["symbol"], str):
        logger.error("Missing or invalid 'symbol' field in market data.")
        return None  # Reject data if symbol is missing

    # Validate 'price'
    if "price" not in data or not isinstance(data["price"], (int, float)):
        logger.warning("Missing or invalid 'price' for symbol %s; marking as partial.", data["symbol"])
        data["price"] = 0.0
        is_partial = True

    # Validate 'volume'
    if "volume" not in data or not isinstance(data["volume"], (int, float)):
        logger.warning("Missing or invalid 'volume' for symbol %s; marking as partial.", data["symbol"])
        data["volume"] = 0
        is_partial = True

    # Validate 'timestamp'
    if "timestamp" not in data:
        logger.warning("Missing 'timestamp' for symbol %s; using current UTC time.", data["symbol"])
        data["timestamp"] = datetime.datetime.utcnow().isoformat()
        is_partial = True
    else:
        try:
            datetime.datetime.fromisoformat(data["timestamp"])
        except Exception as e:
            logger.warning("Malformed 'timestamp' for symbol %s; using current UTC time. Error: %s", data["symbol"], e)
            data["timestamp"] = datetime.datetime.utcnow().isoformat()
            is_partial = True

    if is_partial:
        data["is_partial"] = True

    return data


# --- Primary Polygon API Integration ---
class PolygonService:
    """
    Service class to interact with the primary Polygon API.
    """

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("POLYGON_API_KEY is not set.")
        self.api_key = api_key
        self.base_url = "https://api.polygon.io/v2/last/trade"

    def fetch_market_data(self, symbol: str) -> dict:
        """
        Fetch market data for a given symbol from the primary Polygon API with retry logic.

        Args:
            symbol (str): The ticker symbol to fetch.

        Returns:
            dict: Contains keys 'symbol', 'price', 'volume', 'timestamp'.

        Fallback Mode Trigger:
            - Triggered when the API call fails (e.g., due to rate limiting or missing critical fields)
              after MAX_RETRIES. An exception is raised so that the secondary feed may be tried.
        """
        url = f"{self.base_url}/{symbol}"
        params = {"apiKey": self.api_key}
        retries = 0
        backoff = INITIAL_BACKOFF

        while retries < MAX_RETRIES:
            try:
                logger.info("Primary: Fetching market data for %s", symbol)
                with polygon_api_response_time.labels(symbol=symbol).time():
                    response = requests.get(url, params=params, timeout=5)
                if response.status_code == 429:
                    logger.warning("Primary: Rate limit reached for %s. Retrying after backoff.", symbol)
                    polygon_api_calls.labels(symbol=symbol, status='failure').inc()
                    raise Exception("Rate limited")
                response.raise_for_status()
                data = response.json()
                last_trade = data.get("last", {})
                timestamp_ms = last_trade.get("timestamp")
                if timestamp_ms is None:
                    logger.warning("Primary: Timestamp missing in API response for %s. Using current time.", symbol)
                    timestamp = datetime.datetime.utcnow().isoformat()
                else:
                    timestamp = datetime.datetime.utcfromtimestamp(timestamp_ms / 1000.0).isoformat()
                price = last_trade.get("price")
                if price is None:
                    raise ValueError("Primary: Missing price in API response")
                volume = last_trade.get("size", 0)
                polygon_api_calls.labels(symbol=symbol, status='success').inc()
                return {"symbol": symbol, "price": price, "volume": volume, "timestamp": timestamp}
            except Exception as e:
                retries += 1
                logger.error("Primary: Error fetching data for %s (attempt %s/%s): %s", symbol, retries, MAX_RETRIES, e)
                time.sleep(backoff)
                backoff *= 2  # Exponential backoff

        logger.error("Primary: Failed to fetch data for %s after %s attempts.", symbol, MAX_RETRIES)
        polygon_api_calls.labels(symbol=symbol, status='failure').inc()
        raise Exception("Primary feed failed after max retries")

    def generate_mock_data(self, symbol: str) -> dict:
        """
        Generate fallback mock market data for a given symbol.
        """
        now_utc = datetime.datetime.utcnow().isoformat()
        mock_price = round(random.uniform(100, 200), 2)
        mock_volume = random.randint(100, 1000)
        logger.info("Primary: Using mock data for %s", symbol)
        return {
            "symbol": symbol,
            "price": mock_price,
            "volume": mock_volume,
            "timestamp": now_utc,
            "is_partial": True
        }


# --- Secondary Data Feed Integration ---
class SecondaryPolygonService:
    """
    Service class to interact with the secondary data feed.
    This is a prototype backup endpoint.
    """

    def __init__(self, api_key: str, base_url: str):
        if not api_key:
            raise ValueError("POLYGON_API_KEY is not set for secondary feed.")
        self.api_key = api_key
        self.base_url = base_url

    def fetch_market_data(self, symbol: str) -> dict:
        """
        Fetch market data for a given symbol from the secondary data feed with retry logic.

        Args:
            symbol (str): The ticker symbol to fetch.

        Returns:
            dict: Contains keys 'symbol', 'price', 'volume', 'timestamp'.

        If the secondary feed also fails, an exception is raised.
        """
        url = f"{self.base_url}/{symbol}"
        params = {"apiKey": self.api_key}
        retries = 0
        backoff = INITIAL_BACKOFF

        while retries < MAX_RETRIES:
            try:
                logger.info("Secondary: Fetching market data for %s", symbol)
                with secondary_api_response_time.labels(symbol=symbol).time():
                    response = requests.get(url, params=params, timeout=5)
                if response.status_code == 429:
                    logger.warning("Secondary: Rate limit reached for %s. Retrying after backoff.", symbol)
                    secondary_api_calls.labels(symbol=symbol, status='failure').inc()
                    raise Exception("Rate limited")
                response.raise_for_status()
                data = response.json()
                last_trade = data.get("last", {})
                timestamp_ms = last_trade.get("timestamp")
                if timestamp_ms is None:
                    logger.warning("Secondary: Timestamp missing in API response for %s. Using current time.", symbol)
                    timestamp = datetime.datetime.utcnow().isoformat()
                else:
                    timestamp = datetime.datetime.utcfromtimestamp(timestamp_ms / 1000.0).isoformat()
                price = last_trade.get("price")
                if price is None:
                    raise ValueError("Secondary: Missing price in API response")
                volume = last_trade.get("size", 0)
                secondary_api_calls.labels(symbol=symbol, status='success').inc()
                return {"symbol": symbol, "price": price, "volume": volume, "timestamp": timestamp}
            except Exception as e:
                retries += 1
                logger.error("Secondary: Error fetching data for %s (attempt %s/%s): %s", symbol, retries, MAX_RETRIES, e)
                time.sleep(backoff)
                backoff *= 2

        logger.error("Secondary: Failed to fetch data for %s after %s attempts.", symbol, MAX_RETRIES)
        secondary_api_calls.labels(symbol=symbol, status='failure').inc()
        raise Exception("Secondary feed failed after max retries")


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
        A unique trace_id is added for correlation.
        """
        try:
            message["trace_id"] = str(uuid.uuid4())
            json_message = json.dumps(message)
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue,
                body=json_message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            published_messages.inc()
            logger.info("Published message to RabbitMQ with trace_id: %s", message["trace_id"])
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
    Main loop: for each symbol in the watchlist, attempt to fetch market data from the primary feed.
    On failure, automatically fail over to the secondary feed. If both fail, fallback to mock data.
    Validates the data schema and publishes the resulting event to RabbitMQ.
    Exposes Prometheus metrics via an HTTP endpoint.
    """
    # Start Prometheus metrics server on port 8000
    start_http_server(8000)
    logger.info("Started Prometheus metrics server on port 8000")

    primary_service = PolygonService(api_key=POLYGON_API_KEY)
    secondary_service = SecondaryPolygonService(api_key=POLYGON_API_KEY, base_url=SECONDARY_POLYGON_BASE_URL)
    publisher = RabbitMQPublisher(
        host=RABBITMQ_HOST,
        user=RABBITMQ_USER,
        password=RABBITMQ_PASS,
        queue="market_data"
    )

    try:
        while True:
            for symbol in WATCHLIST:
                try:
                    # Try primary feed first
                    data_event = primary_service.fetch_market_data(symbol)
                except Exception as primary_err:
                    logger.error("Primary feed failed for %s: %s. Failing over to secondary feed.", symbol, primary_err)
                    secondary_failovers.inc()
                    try:
                        data_event = secondary_service.fetch_market_data(symbol)
                    except Exception as secondary_err:
                        logger.error("Secondary feed also failed for %s: %s. Falling back to mock data.", symbol, secondary_err)
                        data_event = primary_service.generate_mock_data(symbol)
                validated_event = validate_market_data(data_event)
                if validated_event is None:
                    logger.error("Data for %s rejected due to missing critical fields.", symbol)
                    continue  # Skip publishing if critical field is missing
                publisher.publish(validated_event)
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
        # Mock a successful primary API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "last": {
                "timestamp": 1609459200000,  # 2021-01-01T00:00:00Z
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
        # Simulate a rate limit response (HTTP 429) for primary service
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_get.return_value = mock_response

        service = PolygonService(api_key="dummy")
        with self.assertRaises(Exception):
            service.fetch_market_data("AAPL")


class TestSecondaryPolygonService(unittest.TestCase):
    @patch("requests.get")
    def test_secondary_fetch_success(self, mock_get):
        # Mock a successful secondary API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "last": {
                "timestamp": 1609459200000,
                "price": 155.0,
                "size": 600
            }
        }
        mock_get.return_value = mock_response

        service = SecondaryPolygonService(api_key="dummy", base_url="https://secondary-api.example.com")
        result = service.fetch_market_data("AAPL")
        self.assertEqual(result["symbol"], "AAPL")
        self.assertEqual(result["price"], 155.0)
        self.assertEqual(result["volume"], 600)
        datetime.datetime.fromisoformat(result["timestamp"])


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