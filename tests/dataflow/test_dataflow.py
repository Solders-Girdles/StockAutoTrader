#!/usr/bin/env python3
"""
test_dataflow.py

A test suite for the production-ready data ingestion script
(dataflow/main.py) from the StockAutoTrader project.
"""

import unittest
import uuid
import json
import logging
import random
import time

# Import datetime and timezone from the datetime module.
from datetime import datetime, timezone

# A sample test function for timestamp conversion (if needed in your tests)
def test_timestamp_conversion(self):
    # Define a sample timestamp in milliseconds.
    timestamp_ms = 1609459200000  # Represents 2021-01-01T00:00:00 UTC
    converted = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).isoformat()
    expected = "2021-01-01T00:00:00+00:00"
    self.assertEqual(converted, expected)

from unittest.mock import patch, MagicMock

# Import the functions and classes to test.
# Adjust the import paths as needed.
from dataflow.main import (
    get_logger,
    validate_market_data,
    PolygonService,
    SecondaryPolygonService,
    RabbitMQPublisher,
    DATA_INTERVAL,
)

# Import JsonFormatter from the common module so we can use it in our log capture.
from common.logging_helper import JsonFormatter


# -----------------------------------------------------------------------------
# A context manager for capturing JSON formatted logs
# -----------------------------------------------------------------------------
class CaptureJsonLogs:
    """
    A context manager to capture logs emitted by a logger using our JSON formatter.
    """

    def __init__(self, logger_name, level="INFO"):
        self.logger_name = logger_name
        self.level = level
        self.records = []
        self.handler = None

    def __enter__(self):
        self.logger = logging.getLogger(self.logger_name)
        # Save the original handlers so we can restore them later.
        self.original_handlers = self.logger.handlers[:]
        # Remove any existing handlers.
        self.logger.handlers = []
        # Create a StreamHandler that writes to this object.
        self.handler = logging.StreamHandler(self)
        # Set the JSON formatter so that the log output is valid JSON.
        self.handler.setFormatter(JsonFormatter())
        self.logger.addHandler(self.handler)
        self.logger.setLevel(getattr(logging, self.level.upper(), self.level))
        return self

    def write(self, message):
        # Write each nonempty line to our records.
        for line in message.splitlines():
            if line.strip():
                self.records.append(line.strip())

    def flush(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        # Restore the original logger handlers.
        self.logger.removeHandler(self.handler)
        self.logger.handlers = self.original_handlers


# -----------------------------------------------------------------------------
# The Test Suite
# -----------------------------------------------------------------------------
class TestDataflow(unittest.TestCase):

    def test_logging_integration(self):
        """
        Verify that logs produced by the DataFlow logger are JSON formatted
        and include the expected correlation_id.
        """
        test_correlation_id = "your-correlation-id"
        logger = get_logger("DataFlow")
        with CaptureJsonLogs("DataFlow", level="INFO") as cap:
            logger.info("Publishing a market data event", extra={'correlation_id': test_correlation_id})

        # Iterate over captured log records and look for the expected correlation_id.
        found = False
        for record in cap.records:
            try:
                log_record = json.loads(record)
            except json.JSONDecodeError:
                continue  # Skip any record that isn't valid JSON.
            if log_record.get("correlation_id") == test_correlation_id:
                found = True
                break

        self.assertTrue(found, f"Log output did not contain the correlation ID: {test_correlation_id}")

    def test_validate_market_data_complete(self):
        """
        If all fields are present and valid, the validated dictionary should match the input.
        """
        data = {
            "symbol": "AAPL",
            "timestamp": "2021-01-01T00:00:00",
            "open": 150,
            "high": 155,
            "low": 149,
            "close": 152,
            "volume": 1000,
            "vwap": 151,
            "interval": "1m"
        }
        validated = validate_market_data(data.copy())
        self.assertEqual(validated, data)

    def test_validate_market_data_partial(self):
        """
        Test that when extended fields are missing (but a "price" is provided),
        the missing OHLC and vwap values are replaced by the price and that the
        data is flagged as partial.
        """
        data = {
            "symbol": "AAPL",
            "timestamp": "2021-01-01T00:00:00",
            "price": 150,
            "volume": 1000,
            "interval": "1m"
        }
        validated = validate_market_data(data.copy())
        self.assertEqual(validated["open"], 150)
        self.assertEqual(validated["high"], 150)
        self.assertEqual(validated["low"], 150)
        self.assertEqual(validated["close"], 150)
        self.assertEqual(validated["vwap"], 150)
        self.assertTrue(validated.get("is_partial"), "Expected data to be marked as partial")

    # Disable retry logic for the following tests by patching tenacity.retry to be an identity decorator.
    @patch("dataflow.main.tenacity.retry", lambda *args, **kwargs: lambda f: f)
    @patch("dataflow.main.requests.get")
    def test_polygon_service_fetch_market_data_with_price(self, mock_get):
        """
        Test PolygonService.fetch_market_data when the API response includes only
        a "price" (i.e. no extended OHLC fields).
        """
        symbol = "AAPL"
        fake_timestamp_ms = 1609459200000  # Represents 2021-01-01T00:00:00 UTC
        fake_response = MagicMock()
        fake_response.status_code = 200
        fake_response.json.return_value = {
            "last": {
                "timestamp": fake_timestamp_ms,
                "price": 150.0,
                "size": 500
            }
        }
        mock_get.return_value = fake_response

        service = PolygonService(api_key="test_api_key")
        result = service.fetch_market_data(symbol)
        expected_timestamp = datetime.fromtimestamp(fake_timestamp_ms / 1000.0, tz=timezone.utc).isoformat()
        expected = {
            "symbol": symbol,
            "timestamp": expected_timestamp,
            "open": 150.0,
            "high": 150.0,
            "low": 150.0,
            "close": 150.0,
            "volume": 500,
            "vwap": 150.0,
            "interval": DATA_INTERVAL
        }
        self.assertEqual(result, expected)

    @patch("dataflow.main.tenacity.retry", lambda *args, **kwargs: lambda f: f)
    @patch("dataflow.main.requests.get")
    def test_polygon_service_fetch_market_data_with_extended_fields(self, mock_get):
        """
        Test PolygonService.fetch_market_data when the API response contains extended fields.
        """
        symbol = "AAPL"
        fake_timestamp_ms = 1609459200000
        fake_response = MagicMock()
        fake_response.status_code = 200
        fake_response.json.return_value = {
            "last": {
                "timestamp": fake_timestamp_ms,
                "open": 149.0,
                "high": 151.0,
                "low": 148.0,
                "close": 150.0,
                "vwap": 150.0,
                "volume": 600
            }
        }
        mock_get.return_value = fake_response

        service = PolygonService(api_key="test_api_key")
        result = service.fetch_market_data(symbol)
        expected_timestamp = datetime.fromtimestamp(fake_timestamp_ms / 1000.0, tz=timezone.utc).isoformat()
        expected = {
            "symbol": symbol,
            "timestamp": expected_timestamp,
            "open": 149.0,
            "high": 151.0,
            "low": 148.0,
            "close": 150.0,
            "volume": 600,
            "vwap": 150.0,
            "interval": DATA_INTERVAL
        }
        self.assertEqual(result, expected)

    @patch("dataflow.main.tenacity.retry", lambda *args, **kwargs: lambda f: f)
    @patch("dataflow.main.requests.get")
    def test_secondary_service_fetch_market_data(self, mock_get):
        """
        Test SecondaryPolygonService.fetch_market_data using a mocked API response.
        """
        symbol = "AAPL"
        fake_timestamp_ms = 1609459200000
        fake_response = MagicMock()
        fake_response.status_code = 200
        fake_response.json.return_value = {
            "last": {
                "timestamp": fake_timestamp_ms,
                "price": 155.0,
                "size": 700
            }
        }
        mock_get.return_value = fake_response

        service = SecondaryPolygonService(api_key="test_api_key", base_url="https://secondary-api.test")
        result = service.fetch_market_data(symbol)
        expected_timestamp = datetime.fromtimestamp(fake_timestamp_ms / 1000.0, tz=timezone.utc).isoformat()
        expected = {
            "symbol": symbol,
            "timestamp": expected_timestamp,
            "open": 155.0,
            "high": 155.0,
            "low": 155.0,
            "close": 155.0,
            "volume": 700,
            "vwap": 155.0,
            "interval": DATA_INTERVAL
        }
        self.assertEqual(result, expected)

    @patch("dataflow.main.pika.BlockingConnection")
    def test_rabbitmq_publisher_connection_and_publish(self, mock_blocking_connection):
        """
        Test that RabbitMQPublisher connects (declaring its queue),
        publishes a message with a generated trace_id, and then closes the connection.
        """
        # Create fake connection and channel objects.
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        # Explicitly mark the connection as open so that close() will be called.
        mock_connection.is_closed = False
        mock_connection.channel.return_value = mock_channel
        mock_blocking_connection.return_value = mock_connection

        publisher = RabbitMQPublisher(host="localhost", user="guest", password="guest", queue="market_data")
        # Verify that queue_declare was called with durable=True.
        mock_channel.queue_declare.assert_called_with(queue="market_data", durable=True)

        # Test publishing a message.
        test_message = {"symbol": "AAPL", "price": 150}
        publisher.publish(test_message)

        # Verify that basic_publish was called.
        args, kwargs = mock_channel.basic_publish.call_args
        self.assertEqual(kwargs["routing_key"], "market_data")
        published_body = kwargs["body"]
        message_dict = json.loads(published_body)
        self.assertIn("trace_id", message_dict)

        publisher.close()
        mock_connection.close.assert_called_once()


# -----------------------------------------------------------------------------
# Run the tests
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    unittest.main()