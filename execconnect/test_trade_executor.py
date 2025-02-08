#!/usr/bin/env python3
"""
test_trade_executor.py

Unit tests for the trade_executor module, using mocks to simulate broker API responses
and validate trade execution logic.
"""

import unittest
from unittest.mock import patch
from datetime import datetime

from trade_executor import execute_trade

class TestTradeExecutor(unittest.TestCase):

    @patch('trade_executor.send_order')
    def test_execute_trade_success(self, mock_send_order):
        trade = {
            "symbol": "AAPL",
            "timestamp": datetime.utcnow().isoformat(),
            "action": "BUY",
            "price": 150.0,
            "quantity": 10,
            "risk_approved": True,
            "correlation_id": "corr-123"
        }
        mock_send_order.return_value = {
            "order_id": "SIM-123",
            "status": "FILLED",
            "filled_quantity": 10,
            "avg_price": 150.0,
            "timestamp": datetime.utcnow().isoformat(),
            "remaining_quantity": 0
        }
        result = execute_trade(trade)
        self.assertEqual(result['status'], "FILLED")
        self.assertEqual(result['filled_quantity'], 10)

    @patch('trade_executor.send_order')
    def test_execute_trade_failure(self, mock_send_order):
        trade = {
            "symbol": "AAPL",
            "timestamp": datetime.utcnow().isoformat(),
            "action": "BUY",
            "price": 150.0,
            "quantity": 10,
            "risk_approved": False,
            "correlation_id": "corr-456"
        }
        mock_send_order.return_value = {
            "order_id": "N/A",
            "status": "FAILED",
            "filled_quantity": 0,
            "avg_price": 0.0,
            "timestamp": datetime.utcnow().isoformat(),
            "remaining_quantity": 10
        }
        result = execute_trade(trade)
        self.assertEqual(result['status'], "FAILED")
        self.assertEqual(result['filled_quantity'], 0)

if __name__ == '__main__':
    unittest.main()