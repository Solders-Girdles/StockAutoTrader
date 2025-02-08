#!/usr/bin/env python3
"""
test_risk.py

Unit tests for the risk module, validating various risk scenarios including:
- Over-leverage,
- Insufficient margin,
- Low confidence,
- Circuit breaker triggers,
- Multi-asset adjustments,
- Extended scenario codes,
- Malformed signals.
"""

import unittest
from risk import evaluate_trade

class TestRiskEvaluator(unittest.TestCase):

    def setUp(self):
        self.portfolio = {
            "AAPL": 100,
            "total_capital": 100000.0,
            "available_margin": 50000.0,
            "daily_drawdown_pct": 0.02
        }

    def test_trade_approved_high_confidence(self):
        signal = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 150.0,
            "desired_quantity": 50,
            "confidence": 0.80,
            "asset_class": "equity"
        }
        self.assertTrue(evaluate_trade(signal, self.portfolio))

    def test_trade_rejected_low_confidence(self):
        signal = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 150.0,
            "desired_quantity": 50,
            "confidence": 0.70,
            "asset_class": "equity"
        }
        self.assertFalse(evaluate_trade(signal, self.portfolio))

    def test_trade_rejected_insufficient_margin(self):
        portfolio_low_margin = {"AAPL": 100, "total_capital": 100000.0, "available_margin": 1000.0, "daily_drawdown_pct": 0.02}
        signal = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 150.0,
            "desired_quantity": 50,
            "confidence": 0.90,
            "asset_class": "equity"
        }
        self.assertFalse(evaluate_trade(signal, portfolio_low_margin))

    def test_trade_rejected_over_position(self):
        signal = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 150.0,
            "desired_quantity": 10000,
            "confidence": 0.95,
            "asset_class": "equity"
        }
        self.assertFalse(evaluate_trade(signal, self.portfolio))

    def test_trade_rejected_exposure_limit(self):
        # Buying additional 100 shares at $150 each would exceed 10% of total capital (i.e., 10000).
        signal = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 150.0,
            "desired_quantity": 100,
            "confidence": 0.95,
            "asset_class": "equity"
        }
        self.assertFalse(evaluate_trade(signal, self.portfolio))

    def test_trade_rejected_stress_test(self):
        signal = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 150.0,
            "desired_quantity": 50,
            "confidence": 0.95,
            "asset_class": "equity"
        }
        # Force stress test rejection by setting a very low threshold.
        self.assertFalse(evaluate_trade(signal, self.portfolio, stress_threshold_pct=0.01))

    def test_trade_rejected_extreme_volatility(self):
        signal = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 150.0,
            "desired_quantity": 50,
            "confidence": 0.95,
            "asset_class": "equity",
            "vix": 35
        }
        self.assertFalse(evaluate_trade(signal, self.portfolio))

    def test_trade_rejected_under_margin_call(self):
        portfolio_under_margin = {"AAPL": 100, "total_capital": 100000.0, "available_margin": 5000.0, "daily_drawdown_pct": 0.02}
        signal = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 150.0,
            "desired_quantity": 10,
            "confidence": 0.95,
            "asset_class": "equity"
        }
        self.assertFalse(evaluate_trade(signal, portfolio_under_margin))

    def test_trade_multi_asset_options(self):
        signal = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 10.0,
            "desired_quantity": 100,
            "confidence": 0.90,
            "asset_class": "options"
        }
        self.assertTrue(evaluate_trade(signal, self.portfolio))

    def test_malformed_signal_missing_symbol(self):
        signal = {
            "action": "BUY",
            "price": 150.0,
            "desired_quantity": 50,
            "confidence": 0.95
        }
        self.assertFalse(evaluate_trade(signal, self.portfolio))

    def test_malformed_signal_negative_price(self):
        signal = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": -150.0,
            "desired_quantity": 50,
            "confidence": 0.95
        }
        self.assertFalse(evaluate_trade(signal, self.portfolio))

if __name__ == "__main__":
    unittest.main()