#!/usr/bin/env python3
"""
test_risk.py

Unit tests for the risk module, validating various risk scenarios such as
over-leverage, insufficient margin, low confidence, and malformed signals.
"""

import unittest
from risk import evaluate_trade

class TestRiskEvaluator(unittest.TestCase):

    def setUp(self):
        # Simulated portfolio for testing.
        self.portfolio = {
            "AAPL": 100,
            "available_margin": 50000.0
        }

    def test_trade_approved_high_confidence(self):
        signal = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 150.0,
            "desired_quantity": 50,
            "confidence": 0.80
        }
        self.assertTrue(evaluate_trade(signal, self.portfolio))

    def test_trade_rejected_low_confidence(self):
        signal = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 150.0,
            "desired_quantity": 50,
            "confidence": 0.70
        }
        self.assertFalse(evaluate_trade(signal, self.portfolio))

    def test_trade_rejected_insufficient_margin(self):
        portfolio_low_margin = {"AAPL": 100, "available_margin": 1000.0}
        signal = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 150.0,
            "desired_quantity": 50,
            "confidence": 0.90
        }
        self.assertFalse(evaluate_trade(signal, portfolio_low_margin))

    def test_trade_rejected_over_position(self):
        signal = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 150.0,
            "desired_quantity": 10000,
            "confidence": 0.95
        }
        self.assertFalse(evaluate_trade(signal, self.portfolio))

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