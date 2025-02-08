#!/usr/bin/env python3
"""
test_trade_executor.py

Unit tests for the trade_executor module, using mocks to simulate broker API responses
and validate trade execution logic including partial fills.
"""

import unittest
from unittest.mock import patch
from datetime import datetime

from trade_executor import execute_trade

class TestTradeExecutor(unittest.TestCase):

    @patch('trade_executor.send_order')
    def test_execute_trade_success_full(self, mock_send_order):
        trade = {
            "symbol": "AAPL",
            "timestamp": datetime.utcnow().isoformat(),
            "action": "BUY",
            "price": 150.0,
            "quantity": 10,
            "risk_approved": True,
            "correlation_id": "corr-123",
            "scenario": "full"
        }
        mock_send_order.return_value = {
            "events": [{
                "order_id": trade.get("order_id", "SIM-123"),
                "status": "FILLED",
                "filled_quantity": 10,
                "avg_price": 150.0,
                "timestamp": datetime.utcnow().isoformat(),
                "remaining_quantity": 0,
                "order_state": "FILLED"
            }]
        }
        events = execute_trade(trade)
        # Expect a pending event plus the filled event.
        self.assertEqual(len(events), 2)
        self.assertEqual(events[1]['status'], "FILLED")
        self.assertEqual(events[1]['filled_quantity'], 10)

    @patch('trade_executor.send_order')
    def test_execute_trade_success_partial(self, mock_send_order):
        trade = {
            "symbol": "AAPL",
            "timestamp": datetime.utcnow().isoformat(),
            "action": "BUY",
            "price": 150.0,
            "quantity": 10,
            "risk_approved": True,
            "correlation_id": "corr-456",
            "scenario": "partial"
        }
        mock_send_order.return_value = {
            "events": [
                {
                    "order_id": trade.get("order_id", "SIM-456"),
                    "status": "PARTIALLY_FILLED",
                    "filled_quantity": 6,
                    "avg_price": 150.05,
                    "timestamp": datetime.utcnow().isoformat(),
                    "remaining_quantity": 4,
                    "order_state": "PARTIALLY_FILLED"
                },
                {
                    "order_id": trade.get("order_id", "SIM-456"),
                    "status": "FILLED",
                    "filled_quantity": 10,
                    "avg_price": 150.0,
                    "timestamp": datetime.utcnow().isoformat(),
                    "remaining_quantity": 0,
                    "order_state": "FILLED"
                }
            ]
        }
        events = execute_trade(trade)
        # Expect a pending event plus two events from partial fill simulation.
        self.assertEqual(len(events), 3)
        self.assertEqual(events[1]['status'], "PARTIALLY_FILLED")
        self.assertEqual(events[2]['status'], "FILLED")
        self.assertEqual(events[1]['filled_quantity'] + (10 - events[1]['filled_quantity']), 10)

    @patch('trade_executor.send_order')
    def test_execute_trade_failure(self, mock_send_order):
        trade = {
            "symbol": "AAPL",
            "timestamp": datetime.utcnow().isoformat(),
            "action": "BUY",
            "price": 150.0,
            "quantity": 10,
            "risk_approved": False,
            "correlation_id": "corr-789",
            "scenario": "full"
        }
        mock_send_order.return_value = {
            "events": [{
                "order_id": trade.get("order_id", "SIM-789"),
                "status": "REJECTED",
                "filled_quantity": 0,
                "avg_price": 150.0,
                "timestamp": datetime.utcnow().isoformat(),
                "remaining_quantity": 10,
                "order_state": "REJECTED"
            }]
        }
        events = execute_trade(trade)
        # Expect a pending event plus the rejected event.
        self.assertEqual(len(events), 2)
        self.assertEqual(events[1]['status'], "REJECTED")
        self.assertEqual(events[1]['filled_quantity'], 0)

if __name__ == '__main__':
    unittest.main()