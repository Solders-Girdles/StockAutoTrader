#!/usr/bin/env python3
"""
dataflow/main.py

A production-ready real-time market data ingestion script for the StockAutoTrader project.

Features:
  - Integrates with the primary Polygon API to fetch market data for multiple symbols.
  - Implements a secondary data source (SecondaryPolygonService) as a backup feed.
  - Automatically fails over to the secondary feed if the primary feed fails or hits rate limits.
  - Expanded market data schema: each published message includes OHLC (open, high, low, close), volume, vwap, and interval along with symbol and timestamp.
  - If extended fields are missing, the single “price” is used to fill all OHLC and vwap fields, and the data is marked as partial.
  - Uses tenacity for exponential backoff and pybreaker for circuit breaking in API calls.
  - Publishes messages to RabbitMQ with durable queues and persistent delivery (delivery_mode=2), and includes a unique trace_id.
  - RabbitMQ connection logic retries up to 10 times if the connection is not initially available.
  - Concurrency is achieved via a ThreadPoolExecutor for parallel processing of symbols.
  - Prometheus metrics (API call counts, response times, published messages, failovers) are exposed on port 8000.
  - Structured JSON logging is used throughout.
  - A consumer mode (--consume) demonstrates message processing with proper ACK/NACK.

Environment Variables:
  - POLYGON_API_KEY: API key for the primary Polygon feed.
  - SECONDARY_POLYGON_BASE_URL: Base URL for the secondary data feed (default: "https://secondary-api.polygon.io/v2/last/trade").
  - RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS: RabbitMQ connection parameters.
  - WATCHLIST: Comma-separated list of symbols (default: "AAPL,MSFT,GOOG,TSLA").
  - DATA_INTERVAL: Interval for market data bars (default: "1m").

Usage:
  To run the publisher:
      python dataflow/main.py

  To run unit tests:
      python dataflow/main.py test

  To run the consumer (for demonstration):
      python dataflow/main.py --consume
"""

import os
import time
import json
from datetime import datetime, timezone
from common.logging_helper import get_logger

# Initialize the logger for DataFlow with the service name
logger = get_logger("DataFlow")

# Now you can use logger.info(), logger.error(), etc.
logger.info("Publishing a market data event", extra={'correlation_id': 'your-correlation-id'})

import random
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor

import requests
import pika
from pika.exceptions import AMQPConnectionError
from pythonjsonlogger import jsonlogger

# Prometheus client imports
from prometheus_client import start_http_server, Counter, Histogram

# Import tenacity for retry/backoff and pybreaker for circuit breaker
import tenacity
import pybreaker

# For unit testing
import unittest
from unittest.mock import patch, MagicMock

# --- Environment Variables & Configuration ---
POLYGON_API_KEY = os.environ.get("d5Wst8dKFQRQ8yTJpsPiV1a9wzHzwF4K")
SECONDARY_POLYGON_BASE_URL = os.environ.get("SECONDARY_POLYGON_BASE_URL", "https://secondary-api.polygon.io/v2/last/trade")
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")
WATCHLIST = os.environ.get("WATCHLIST", "AAPL,MSFT,GOOG,TSLA").split(",")
DATA_INTERVAL = os.environ.get("DATA_INTERVAL", "1m")

# Retry configuration for API calls
MAX_RETRIES = 5
INITIAL_BACKOFF = 1  # seconds

# --- Prometheus Metrics ---
polygon_api_calls = Counter('polygon_api_calls_total', 'Total calls to primary Polygon API', ['symbol', 'status'])
secondary_api_calls = Counter('secondary_api_calls_total', 'Total calls to secondary data feed', ['symbol', 'status'])
polygon_api_response_time = Histogram('polygon_api_response_time_seconds', 'Response time for primary API (s)', ['symbol'])
secondary_api_response_time = Histogram('secondary_api_response_time_seconds', 'Response time for secondary API (s)', ['symbol'])
published_messages = Counter('published_messages_total', 'Total messages published to RabbitMQ')
publishing_errors = Counter('publishing_errors_total', 'Total publishing errors to RabbitMQ')
secondary_failovers = Counter('secondary_failovers_total', 'Total number of failovers to secondary feed')

# --- Circuit Breakers ---
primary_breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=60)
secondary_breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=60)

# --- Schema Validation Function ---
def validate_market_data(data: dict) -> dict:
    """
    Validate and expand the market data dictionary.
    Expected fields:
      - symbol (str)
      - timestamp (ISO8601 str)
      - open, high, low, close (numbers)
      - volume (number)
      - vwap (number)
      - interval (str)
    Fills in missing fields with defaults (using price if needed) and flags data as partial if any fields are missing.
    """
    is_partial = False

    if "symbol" not in data or not isinstance(data["symbol"], str):
        logger.error("Missing or invalid 'symbol'.")
        return None

    for field in ["open", "high", "low", "close", "vwap"]:
        if field not in data:
            if "price" in data:
                data[field] = data["price"]
                is_partial = True  # Mark as partial because the extended field was missing.
            else:
                logger.warning("Missing '%s' for %s; marking as partial.", field, data["symbol"])
                data[field] = 0.0
                is_partial = True

    if "volume" not in data or not isinstance(data["volume"], (int, float)):
        logger.warning("Missing or invalid 'volume' for %s; marking as partial.", data["symbol"])
        data["volume"] = 0
        is_partial = True

    if "timestamp" not in data:
        logger.warning("Missing 'timestamp' for %s; using current time.", data["symbol"])
        data["timestamp"] = datetime.datetime.utcnow().isoformat()
        is_partial = True
    else:
        try:
            datetime.datetime.fromisoformat(data["timestamp"])
        except Exception as e:
            logger.warning("Malformed 'timestamp' for %s; using current time. Error: %s", data["symbol"], e)
            data["timestamp"] = datetime.datetime.utcnow().isoformat()
            is_partial = True

    if "interval" not in data or not isinstance(data["interval"], str):
        data["interval"] = DATA_INTERVAL

    if is_partial:
        data["is_partial"] = True

    return data


# --- Primary Polygon API Integration ---
class PolygonService:
    """
    Service to fetch market data from the primary Polygon API.
    """
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("POLYGON_API_KEY is not set.")
        self.api_key = api_key
        self.base_url = "https://api.polygon.io/v2/last/trade"

    @primary_breaker
    @tenacity.retry(stop=tenacity.stop_after_attempt(MAX_RETRIES),
                    wait=tenacity.wait_exponential(multiplier=INITIAL_BACKOFF),
                    retry=tenacity.retry_if_exception_type(Exception),
                    reraise=True)
    def fetch_market_data(self, symbol: str) -> dict:
        """
        Fetch market data from the primary API.
        Returns an expanded dict with OHLC, volume, vwap, and interval.
        If extended fields are not available, uses the single 'price' value.
        """
        url = f"{self.base_url}/{symbol}"
        params = {"apiKey": self.api_key}
        logger.info("Primary: Fetching data for %s", symbol)
        with polygon_api_response_time.labels(symbol=symbol).time():
            response = requests.get(url, params=params, timeout=5)
        if response.status_code == 429:
            logger.warning("Primary: Rate limit reached for %s.", symbol)
            polygon_api_calls.labels(symbol=symbol, status='failure').inc()
            raise Exception("Rate limited")
        response.raise_for_status()
        data = response.json()
        last_trade = data.get("last", {})
        timestamp_ms = last_trade.get("timestamp")
        if timestamp_ms is None:
            logger.warning("Primary: Timestamp missing for %s; using current time.", symbol)
            timestamp = datetime.datetime.utcnow().isoformat()
        else:
            timestamp = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).isoformat()
        if "open" in last_trade:
            open_val = last_trade.get("open")
            high_val = last_trade.get("high")
            low_val = last_trade.get("low")
            close_val = last_trade.get("close")
            vwap_val = last_trade.get("vwap", close_val)
            volume = last_trade.get("volume", last_trade.get("size", 0))
        else:
            price = last_trade.get("price")
            if price is None:
                raise ValueError("Primary: Missing price in API response")
            open_val = high_val = low_val = close_val = vwap_val = price
            volume = last_trade.get("size", 0)
        polygon_api_calls.labels(symbol=symbol, status='success').inc()
        return {
            "symbol": symbol,
            "timestamp": timestamp,
            "open": open_val,
            "high": high_val,
            "low": low_val,
            "close": close_val,
            "volume": volume,
            "vwap": vwap_val,
            "interval": DATA_INTERVAL
        }

    def generate_mock_data(self, symbol: str) -> dict:
        """
        Generate fallback mock market data with the expanded schema.
        """
        now_utc = datetime.datetime.utcnow().isoformat()
        price = round(random.uniform(100, 200), 2)
        volume = random.randint(100, 1000)
        logger.info("Primary: Using mock data for %s", symbol)
        return {
            "symbol": symbol,
            "timestamp": now_utc,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": volume,
            "vwap": price,
            "interval": DATA_INTERVAL,
            "is_partial": True
        }


# --- Secondary Data Feed Integration ---
class SecondaryPolygonService:
    """
    Service to fetch market data from the secondary data feed.
    """
    def __init__(self, api_key: str, base_url: str):
        if not api_key:
            raise ValueError("POLYGON_API_KEY is not set for secondary feed.")
        self.api_key = api_key
        self.base_url = base_url

    @secondary_breaker
    @tenacity.retry(stop=tenacity.stop_after_attempt(MAX_RETRIES),
                    wait=tenacity.wait_exponential(multiplier=INITIAL_BACKOFF),
                    retry=tenacity.retry_if_exception_type(Exception),
                    reraise=True)
    def fetch_market_data(self, symbol: str) -> dict:
        """
        Fetch market data from the secondary feed.
        Returns an expanded dict with OHLC, volume, vwap, and interval.
        """
        url = f"{self.base_url}/{symbol}"
        params = {"apiKey": self.api_key}
        logger.info("Secondary: Fetching data for %s", symbol)
        with secondary_api_response_time.labels(symbol=symbol).time():
            response = requests.get(url, params=params, timeout=5)
        if response.status_code == 429:
            logger.warning("Secondary: Rate limit reached for %s.", symbol)
            secondary_api_calls.labels(symbol=symbol, status='failure').inc()
            raise Exception("Rate limited")
        response.raise_for_status()
        data = response.json()
        last_trade = data.get("last", {})
        timestamp_ms = last_trade.get("timestamp")
        if timestamp_ms is None:
            logger.warning("Secondary: Timestamp missing for %s; using current time.", symbol)
            timestamp = datetime.datetime.utcnow().isoformat()
        else:
            datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).isoformat()
        if "open" in last_trade:
            open_val = last_trade.get("open")
            high_val = last_trade.get("high")
            low_val = last_trade.get("low")
            close_val = last_trade.get("close")
            vwap_val = last_trade.get("vwap", close_val)
            volume = last_trade.get("volume", last_trade.get("size", 0))
        else:
            price = last_trade.get("price")
            if price is None:
                raise ValueError("Secondary: Missing price in API response")
            open_val = high_val = low_val = close_val = vwap_val = price
            volume = last_trade.get("size", 0)
        secondary_api_calls.labels(symbol=symbol, status='success').inc()
        return {
            "symbol": symbol,
            "timestamp": timestamp,
            "open": open_val,
            "high": high_val,
            "low": low_val,
            "close": close_val,
            "volume": volume,
            "vwap": vwap_val,
            "interval": DATA_INTERVAL
        }


# --- RabbitMQ Publisher ---
class RabbitMQPublisher:
    """
    Publisher to send messages to RabbitMQ.
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
        Establish connection to RabbitMQ with up to 10 retries (5-second intervals).
        """
        attempts = 0
        while attempts < 10:
            try:
                credentials = pika.PlainCredentials(self.user, self.password)
                parameters = pika.ConnectionParameters(host=self.host, credentials=credentials)
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue=self.queue, durable=True)
                logger.info("Connected to RabbitMQ at %s on attempt %s", self.host, attempts + 1)
                return
            except AMQPConnectionError as e:
                attempts += 1
                logger.error("Failed to connect to RabbitMQ (attempt %s/10): %s", attempts, e)
                time.sleep(5)
        raise AMQPConnectionError("Failed to connect to RabbitMQ after 10 attempts")

    def publish(self, message: dict):
        """
        Publish a JSON message to RabbitMQ with persistent delivery and a unique trace_id.
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
            logger.info("Published message with trace_id: %s", message["trace_id"])
        except Exception as e:
            publishing_errors.inc()
            logger.error("Failed to publish message: %s", e)
            raise

    def close(self):
        """
        Close the RabbitMQ connection.
        """
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("RabbitMQ connection closed.")


# --- RabbitMQ Consumer (for demonstration) ---
def consume_messages():
    """
    Consume messages from the market_data queue.
    Acknowledge each message on success; nack on error.
    """
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue="market_data", durable=True)
        logger.info("Consumer: Connected to RabbitMQ at %s", RABBITMQ_HOST)

        def callback(ch, method, properties, body):
            try:
                message = json.loads(body)
                logger.info("Consumed message: %s", message)
                # Process the message here...
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logger.error("Error processing message: %s", e)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        channel.basic_consume(queue="market_data", on_message_callback=callback)
        logger.info("Consumer: Waiting for messages. To exit press CTRL+C")
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Consumer: Interrupted by user, exiting.")
    except Exception as e:
        logger.error("Consumer: Unexpected error: %s", e)


# --- Main Publisher Function with Concurrency ---
def main_publisher():
    """
    Main loop: concurrently fetch expanded market data for each symbol,
    fail over to secondary or mock data as needed, validate the schema,
    and publish the event to RabbitMQ.
    Prometheus metrics are exposed on port 8000.
    """
    start_http_server(8000)
    logger.info("Started Prometheus metrics server on port 8000")

    primary_service = PolygonService("d5Wst8dKFQRQ8yTJpsPiV1a9wzHzwF4K")
    secondary_service = SecondaryPolygonService(api_key="SpxTa5t3VR9J1sXVW1vxfjnGafcSxe4g", base_url=SECONDARY_POLYGON_BASE_URL)
    publisher = RabbitMQPublisher(host=RABBITMQ_HOST, user=RABBITMQ_USER, password=RABBITMQ_PASS, queue="market_data")

    def process_symbol(symbol: str):
        try:
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
            return
        publisher.publish(validated_event)

    with ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            futures = [executor.submit(process_symbol, symbol) for symbol in WATCHLIST]
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error("Error processing symbol: %s", e)
            time.sleep(5)
    publisher.close()


# --- Main Entry Point ---
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--consume":
        consume_messages()
    elif len(sys.argv) > 1 and sys.argv[1] == "test":
        sys.argv.pop(1)
        unittest.main()
    else:
        main_publisher()