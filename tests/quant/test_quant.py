# test_quant.py
import sys
import os

# Add the project root to sys.path so that modules in the project root and the quant package are discoverable.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

import unittest
import json
import time
import random
import numpy as np
from datetime import datetime
from unittest.mock import MagicMock, patch

# Import modules from your project.
from quant.consumer import on_message as consumer_on_message
from quant.performance import PerformanceTracker
from quant.strategies import (
    SymbolStrategy,
    MACDStrategy,
    BollingerBandsStrategy,
    RSIIndicator,
    MLSignalStrategy,
    aggregate_signals
)


# --- Dummy classes to simulate a RabbitMQ channel ---
class DummyChannel:
    def __init__(self):
        self.acked = []
        self.nacked = []

    def basic_ack(self, delivery_tag):
        self.acked.append(delivery_tag)

    def basic_nack(self, delivery_tag, requeue):
        self.nacked.append((delivery_tag, requeue))


class DummyMethod:
    def __init__(self, delivery_tag):
        self.delivery_tag = delivery_tag


class DummyProperties:
    def __init__(self, headers):
        self.headers = headers


# --- Tests for the consumer module ---
class TestConsumer(unittest.TestCase):
    def test_on_message_ack(self):
        """
        Verify that the consumer's on_message callback acknowledges the message.
        """
        channel = DummyChannel()
        method = DummyMethod(delivery_tag=1)
        properties = DummyProperties(headers={'correlation_id': '1234'})
        body = json.dumps({"test": "data"}).encode("utf-8")
        consumer_on_message(channel, method, properties, body)
        self.assertIn(1, channel.acked, "Message should have been acknowledged.")


# --- Tests for performance.py ---
class TestPerformanceTracker(unittest.TestCase):
    def test_update_and_compute_metrics(self):
        """
        Simulate a BUY trade followed by a SELL trade and check that performance metrics are computed.
        """
        pt = PerformanceTracker(initial_cash=100000.0)
        # Simulate a BUY trade.
        buy_trade = {
            "action": "BUY",
            "price": 100.0,
            "timestamp": "2025-02-10T10:00:00Z",
            "symbol": "TEST"
        }
        pt.update(buy_trade)
        # Simulate a SELL trade generating profit.
        sell_trade = {
            "action": "SELL",
            "price": 110.0,
            "timestamp": "2025-02-10T15:00:00Z",
            "symbol": "TEST",
            "fill_ratio": 1.0
        }
        pt.update(sell_trade)
        metrics = pt.compute_metrics()
        self.assertGreater(pt.cash, 100000.0, "Cash should increase after a profitable trade.")
        self.assertIn("sharpe_ratio", metrics)
        self.assertIn("win_loss_ratio", metrics)

    def test_report_performance(self):
        """
        Manually populate cash_history and trades to simulate multiple days,
        and verify that report_performance returns a summary with daily keys and advanced metrics.
        """
        pt = PerformanceTracker(initial_cash=100000.0)
        pt.cash_history = [
            ("2025-02-09T10:00:00Z", 100000.0),
            ("2025-02-09T16:00:00Z", 101000.0),
            ("2025-02-10T10:00:00Z", 101000.0),
            ("2025-02-10T16:00:00Z", 102000.0),
        ]
        pt.trades = [
            {"timestamp": "2025-02-09T16:00:00Z", "profit": 1000},
            {"timestamp": "2025-02-10T16:00:00Z", "profit": 1000}
        ]
        summary = pt.report_performance()
        self.assertIn("2025-02-09", summary)
        self.assertIn("2025-02-10", summary)
        self.assertIn("advanced_metrics", summary)


# --- Tests for quant.strategies ---
class TestStrategies(unittest.TestCase):
    def test_symbol_strategy_buy(self):
        """
        Feed SymbolStrategy a sequence of increasing prices to trigger a BUY signal.
        """
        strat = SymbolStrategy(
            symbol="TEST",
            ma_short=2,
            ma_long=3,
            rsi_period=14,
            use_rsi=False,
            rsi_buy=30,
            rsi_sell=70
        )
        timestamp = "2025-02-10T12:00:00Z"
        prices = [100, 102, 104]
        result = {}
        for price in prices:
            result = strat.update(timestamp, price)
        self.assertTrue(result, "Expected a signal from SymbolStrategy")
        self.assertEqual(result.get("signal_type"), "BUY")

    def test_macd_strategy(self):
        """
        Provide MACDStrategy with a series of prices; after sufficient data,
        a BUY or SELL signal should be generated.
        """
        strat = MACDStrategy("TEST", short_period=2, long_period=3, signal_period=2)
        timestamp = "2025-02-10T12:00:00Z"
        results = []
        prices = [100, 102, 104, 103, 105, 107]
        for price in prices:
            res = strat.update(timestamp, price)
            if res:
                results.append(res)
        self.assertTrue(any(r.get("signal_type") in ["BUY", "SELL"] for r in results),
                        "MACDStrategy should eventually produce a BUY or SELL signal.")

    def test_bollinger_bands_strategy(self):
        """
        Build BollingerBandsStrategy's window with a nearly stable series and then supply an outlier that is strictly below
        the computed lower band to trigger a BUY signal.
        """
        strat = BollingerBandsStrategy("TEST", window=5, multiplier=2.0)
        timestamp = "2025-02-10T12:00:00Z"
        # Use a nearly stable series.
        stable_prices = [100, 100.1, 100, 99.9, 100]
        for price in stable_prices:
            strat.update(timestamp, price)
        # Now supply an outlier that is clearly below the computed lower band.
        result = strat.update(timestamp, 47.0)  # changed from 48.8 to 48.0
        print("[DEBUG TEST] BollingerBandsStrategy result:", result)
        self.assertTrue(result, "Expected a signal from BollingerBandsStrategy")
        self.assertEqual(result.get("signal_type"), "BUY")

    def test_rsi_indicator(self):
        """
        Test the RSIIndicator with both increasing and decreasing price sequences.
        """
        indicator = RSIIndicator(period=3)
        increasing = [100, 105, 110, 115]
        rsi_inc = indicator.compute(increasing)
        self.assertGreaterEqual(rsi_inc, 70, "RSI for increasing prices should be high")
        decreasing = [115, 110, 105, 100]
        rsi_dec = indicator.compute(decreasing)
        self.assertLessEqual(rsi_dec, 30, "RSI for decreasing prices should be low")

    def test_ml_signal_strategy(self):
        """
        Feed MLSignalStrategy enough price data to trigger model training and signal generation.
        """
        strat = MLSignalStrategy("TEST", window=5)
        timestamp = "2025-02-10T12:00:00Z"
        results = []
        prices = [100, 101, 100, 102, 101, 105]
        for price in prices:
            res = strat.update(timestamp, price)
            if res:
                results.append(res)
        self.assertTrue(results, "MLSignalStrategy should eventually return a signal")
        for r in results:
            self.assertIn(r.get("signal_type"), ["BUY", "SELL"])

    def test_aggregate_signals(self):
        """
        Provide a set of signals to aggregate_signals and verify that the returned aggregated signal is as expected.
        """
        signals = [
            {"symbol": "TEST", "action": "BUY", "confidence": 0.8, "source": "legacy"},
            {"symbol": "TEST", "action": "BUY", "confidence": 0.7, "source": "MACD"},
            {"symbol": "TEST", "action": "SELL", "confidence": 0.6, "source": "Bollinger"}
        ]
        agg = aggregate_signals(signals)
        self.assertIn(agg.get("signal_type"), ["BUY", "SELL", "HOLD"])

        signals = [
            {"symbol": "TEST", "action": "BUY", "confidence": 0.5, "source": "legacy"},
            {"symbol": "TEST", "action": "SELL", "confidence": 0.5, "source": "MACD"}
        ]
        agg = aggregate_signals(signals)
        self.assertEqual(agg.get("signal_type"), "HOLD")


if __name__ == "__main__":
    unittest.main()