#!/usr/bin/env python3
"""
test_riskops.py

A comprehensive test suite for the RiskOps service in the StockAutoTrader project.
This suite covers:
  - Risk evaluation logic in riskops/main.py
  - Daily risk reporting in riskops/reporting.py
  - RabbitMQ publishing utility in riskops/mq_util.py (with simulated connections)
  - Trade signal processing in riskops/main.py
  - The monitoring HTTP endpoint in riskops (if applicable)
"""

import unittest
import json
import time
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

import sys
import os

# Ensure the repository root is on the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Update the imports to match the repository structure.
from riskops.main import evaluate_trade, process_trade_signal, portfolio_data, consume_trade_signals, on_message_callback
from riskops.reporting import generate_daily_risk_report
from riskops.mq_util import publish_message

import pika  # Used for exception simulation


# ================================
# Tests for riskops/main.py (Risk Evaluation)
# ================================
class TestRiskEvaluation(unittest.TestCase):
    def setUp(self):
        # A default valid signal and portfolio.
        self.signal = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 100,  # dollars
            "desired_quantity": 50,
            "confidence": 0.8,
            "asset_class": "equity",
            "vix": 25  # below extreme volatility threshold of 30
        }
        self.portfolio = {
            "AAPL": 0,  # no current position
            "total_capital": 100000,
            "available_margin": 50000,
            "daily_drawdown_pct": 0.02  # 2%
        }

    def test_valid_trade(self):
        """A valid BUY signal should be approved."""
        result = evaluate_trade(self.signal, self.portfolio)
        self.assertTrue(result)

    def test_low_confidence(self):
        """Signal with confidence below threshold (0.75) should be rejected."""
        self.signal["confidence"] = 0.7
        result = evaluate_trade(self.signal, self.portfolio)
        self.assertFalse(result)

    def test_circuit_breaker(self):
        """If portfolio drawdown equals/exceeds the circuit breaker threshold, reject."""
        self.portfolio["daily_drawdown_pct"] = 0.05  # threshold is 0.05
        result = evaluate_trade(self.signal, self.portfolio)
        self.assertFalse(result)

    def test_extreme_volatility(self):
        """Signal with VIX above 30 should be rejected (extreme volatility)."""
        self.signal["vix"] = 35
        result = evaluate_trade(self.signal, self.portfolio)
        self.assertFalse(result)

    def test_under_margin_call(self):
        """If available margin is less than 10% of total capital, reject the signal."""
        self.portfolio["available_margin"] = 5000  # less than 10% of 100000
        result = evaluate_trade(self.signal, self.portfolio)
        self.assertFalse(result)

    def test_over_position_limit(self):
        """A BUY signal that would exceed the per‑symbol maximum position should be rejected."""
        self.portfolio["AAPL"] = 990  # near the limit of 1000 (default)
        self.signal["desired_quantity"] = 20  # new total = 1010 > 1000
        result = evaluate_trade(self.signal, self.portfolio)
        self.assertFalse(result)

    def test_exposure_limit_breach(self):
        """
        For equities, the max exposure is 10% of total capital.
        Increase the price/quantity so that the new exposure > allowed exposure.
        """
        # Allowed max_exposure = 100000 * 0.10 = 10,000.
        self.signal["price"] = 200   # exposure per unit is higher
        self.signal["desired_quantity"] = 60  # 60*200 = 12,000 > 10,000 allowed
        result = evaluate_trade(self.signal, self.portfolio)
        self.assertFalse(result)

    def test_stress_test_fail(self):
        """
        Override stress_threshold_pct to simulate stress failure.
        For a valid equity order with new_exposure = 60*100 = 6000,
        the stress exposure = 6000 * 0.10 = 600.
        With a very low threshold (e.g. 0.005 so allowed = 500),
        the trade should be rejected.
        """
        self.signal["price"] = 100
        self.signal["desired_quantity"] = 60  # new exposure = 6000
        result = evaluate_trade(self.signal, self.portfolio, stress_threshold_pct=0.005)
        self.assertFalse(result)


# ================================
# Tests for riskops/reporting.py (Risk Report Generation)
# ================================
class TestReporting(unittest.TestCase):
    def test_generate_daily_risk_report(self):
        portfolio = {
            "AAPL": 100,
            "MSFT": 50,
            "total_capital": 100000,
            "available_margin": 40000,
            "daily_drawdown_pct": 0.03
        }
        report = generate_daily_risk_report(portfolio)
        self.assertIn("report_timestamp", report)
        self.assertIn("portfolio_exposures_by_asset", report)
        self.assertIn("var_trend", report)
        self.assertIn("stress_test_outcome", report)
        self.assertIn("circuit_breaker_hits", report)
        self.assertIn("daily_drawdown_pct", report)

        # Check that the exposures only include symbols (skip total_capital, etc.)
        exposures = report["portfolio_exposures_by_asset"].get("equity", {})
        self.assertIn("AAPL", exposures)
        self.assertIn("MSFT", exposures)
        # Ensure the VaR trend covers 7 days.
        self.assertEqual(len(report["var_trend"]), 7)


# ================================
# Tests for riskops/mq_util.py (RabbitMQ Publishing)
# ================================
# Create fake connection and channel classes to simulate RabbitMQ behavior.
class FakeChannel:
    def queue_declare(self, queue, durable):
        self.declared_queue = queue
        self.durable = durable

    def basic_publish(self, exchange, routing_key, body, properties):
        self.published = True
        self.exchange = exchange
        self.routing_key = routing_key
        self.body = body
        self.properties = properties

class FakeConnection:
    def __init__(self):
        self.channel_instance = FakeChannel()

    def channel(self):
        return self.channel_instance

    def close(self):
        pass

class TestMQUtil(unittest.TestCase):
    @patch("riskops.mq_util.pika.BlockingConnection", return_value=FakeConnection())
    def test_publish_message_success(self, mock_connection):
        queue_name = "test_queue"
        message = {"key": "value"}
        publish_message(queue_name, message, max_retries=1)
        # Retrieve the fake channel that was used.
        connection = mock_connection.return_value
        channel = connection.channel_instance
        self.assertTrue(hasattr(channel, "published"))
        self.assertEqual(channel.routing_key, queue_name)
        self.assertEqual(json.loads(channel.body), message)

    @patch("riskops.mq_util.pika.BlockingConnection", side_effect=pika.exceptions.AMQPConnectionError("Connection error"))
    def test_publish_message_failure(self, mock_connection):
        queue_name = "test_queue"
        message = {"key": "value"}
        # Call publish_message with a very short delay so the test runs fast.
        publish_message(queue_name, message, max_retries=2, initial_delay=0)
        # Ensure that the BlockingConnection was attempted the expected number of times.
        self.assertEqual(mock_connection.call_count, 2)


# ================================
# Tests for riskops/main.py (Trade Signal Processing)
# ================================
class TestMainProcessTrade(unittest.TestCase):
    def setUp(self):
        # Backup the original portfolio data.
        self.original_portfolio = portfolio_data.copy()
        # Set a known starting portfolio.
        portfolio_data.clear()
        portfolio_data.update({
            "AAPL": 0,
            "total_capital": 100000,
            "available_margin": 50000,
            "daily_drawdown_pct": 0.02
        })
        self.sample_signal = {
            "symbol": "AAPL",
            "timestamp": "2025-02-07T12:00:00Z",
            "action": "BUY",
            "price": 150.0,
            "desired_quantity": 50,
            "confidence": 0.80,
            "asset_class": "equity",
            "vix": 25,
            "correlation_id": "test123"
        }

    def tearDown(self):
        # Restore portfolio data.
        portfolio_data.clear()
        portfolio_data.update(self.original_portfolio)

    @patch("riskops.main.publish_message")
    def test_process_trade_signal_approved(self, mock_publish):
        """A valid BUY signal should update the portfolio and call publish_message."""
        process_trade_signal(self.sample_signal, correlation_id="test123")
        # For a BUY order, portfolio_data["AAPL"] should increase by desired_quantity.
        self.assertEqual(portfolio_data["AAPL"], 50)
        # Calculate expected margin usage for equities (leverage_limit = 2.0).
        margin_used = (150.0 * 50) / 2.0  # = 3750
        self.assertAlmostEqual(portfolio_data["available_margin"], 50000 - margin_used)
        # Verify that publish_message was called with the approved trade message.
        mock_publish.assert_called_once()
        args, kwargs = mock_publish.call_args
        approved_trade_message = args[1]
        self.assertTrue(approved_trade_message.get("risk_approved", False))

    @patch("riskops.main.publish_message")
    def test_process_trade_signal_rejected(self, mock_publish):
        """A signal that is rejected (e.g. due to low confidence) should not update the portfolio."""
        self.sample_signal["confidence"] = 0.7  # Below the threshold
        process_trade_signal(self.sample_signal, correlation_id="test123")
        # Since the trade is rejected, the position should not change.
        self.assertEqual(portfolio_data.get("AAPL", 0), 0)
        # publish_message should not be called because the trade was not approved.
        mock_publish.assert_not_called()


# ================================
# Tests for riskops/monitoring.py (HTTP Endpoint)
# ================================
# If you have a monitoring endpoint within riskops, adjust accordingly.
# If the monitoring endpoint is provided by another service, you might skip this section.
# For demonstration, here's a hypothetical test if there is a Flask app in riskops/monitoring.py.

try:
    from riskops import monitoring
except ImportError:
    monitoring = None

if monitoring is not None:
    class TestMonitoringEndpoint(unittest.TestCase):
        def setUp(self):
            self.app = monitoring.app.test_client()

        def test_metrics_endpoint(self):
            response = self.app.get("/metrics")
            self.assertEqual(response.status_code, 200)
            data = response.get_json()
            # Check for key metrics in the JSON response.
            self.assertIn("portfolio_exposures", data)
            self.assertIn("active_var", data)
            self.assertIn("circuit_breaker_triggered", data)
            self.assertIn("historical_drawdowns", data)


# ================================
# Run the tests
# ================================
if __name__ == "__main__":
    unittest.main()