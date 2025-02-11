import sys
import os
import copy

# Add the repository root (which contains the execconnect package) to sys.path.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import unittest
from unittest.mock import patch

# Import the production modules using the correct (lowercase) package name.
from execconnect.trade_executor import execute_trade
from execconnect.broker_api import simulate_order, send_order


class TestTradeExecutor(unittest.TestCase):
    @patch("execconnect.trade_executor.send_order")
    def test_execute_trade_success(self, mock_send_order):
        # Prepare a simulated execution event that send_order should return.
        simulated_event = {
            "order_id": "dummy",  # This value will be overwritten by execute_trade.
            "status": "FILLED",
            "filled_quantity": 50,
            "avg_price": 154,
            "timestamp": "2025-02-11T04:44:31.899001",
            "remaining_quantity": 0,
            "order_state": "FILLED"
        }
        # When send_order is called, return a dict with an "events" key.
        mock_send_order.return_value = {"events": [simulated_event]}

        # Construct a trade order.
        trade = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 154,
            "quantity": 50,
            "risk_approved": True,
            "correlation_id": "order123"
        }

        # execute_trade logs a PENDING event and then calls send_order.
        events = execute_trade(trade)

        # We expect two events: one PENDING and one from send_order.
        self.assertEqual(len(events), 2)
        pending_event = events[0]
        execution_event = events[1]

        # Verify the PENDING event.
        self.assertEqual(pending_event["status"], "PENDING")
        self.assertEqual(
            pending_event["order_id"],
            execution_event["order_id"],
            "The order_id should be consistent across events."
        )

        # Verify the simulated execution event.
        self.assertEqual(execution_event["status"], "FILLED")
        self.assertEqual(execution_event["filled_quantity"], 50)

    @patch("execconnect.trade_executor.send_order")
    def test_execute_trade_failure(self, mock_send_order):
        # Simulate an error from send_order.
        mock_send_order.side_effect = Exception("Broker API failure")

        trade = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 154,
            "quantity": 50,
            "risk_approved": True,
            "correlation_id": "order456"
        }

        with self.assertRaises(Exception) as context:
            execute_trade(trade)
        self.assertIn("Broker API failure", str(context.exception))


class TestBrokerAPISimulateOrder(unittest.TestCase):
    def test_simulate_order_full_filled(self):
        trade = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 154,
            "quantity": 50,
            "risk_approved": True,
            "order_id": "order123"
        }
        result = simulate_order(trade)
        self.assertIn("events", result)
        events = result["events"]
        self.assertEqual(len(events), 1)

        event = events[0]
        self.assertEqual(event["status"], "FILLED")
        self.assertEqual(event["filled_quantity"], 50)
        self.assertEqual(event["order_state"], "FILLED")
        self.assertEqual(event["order_id"], "order123")

    def test_simulate_order_rejected(self):
        trade = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 154,
            "quantity": 50,
            "risk_approved": False,
            "order_id": "order123"
        }
        result = simulate_order(trade)
        events = result["events"]
        self.assertEqual(len(events), 1)

        event = events[0]
        self.assertEqual(event["status"], "REJECTED")
        self.assertEqual(event["filled_quantity"], 0)
        self.assertEqual(event["order_state"], "REJECTED")
        self.assertEqual(event["order_id"], "order123")

    def test_simulate_order_partial(self):
        trade = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 154,
            "quantity": 50,
            "scenario": "partial",
            "order_id": "order123"
        }
        result = simulate_order(trade)
        events = result["events"]
        self.assertEqual(len(events), 2)

        first_event = events[0]
        second_event = events[1]

        self.assertEqual(first_event["status"], "PARTIALLY_FILLED")
        self.assertEqual(first_event["filled_quantity"], int(50 * 0.6))
        self.assertEqual(first_event["order_state"], "PARTIALLY_FILLED")

        self.assertEqual(second_event["status"], "FILLED")
        self.assertEqual(second_event["filled_quantity"], 50)
        self.assertEqual(second_event["order_state"], "FILLED")

        # Both events must share the same order_id.
        self.assertEqual(first_event["order_id"], "order123")
        self.assertEqual(second_event["order_id"], "order123")


class TestBrokerAPISendOrderSimulation(unittest.TestCase):
    @patch("execconnect.broker_api.ALPACA_API_KEY", new=None)
    @patch("execconnect.broker_api.tradeapi", new=None)
    def test_send_order_simulation(self):
        """
        When the broker API is not available (i.e. tradeapi is None or ALPACA_API_KEY is not set),
        send_order should fall back to simulate_order.
        """
        trade = {
            "symbol": "AAPL",
            "action": "BUY",
            "price": 154,
            "quantity": 50,
            "risk_approved": True,
            "order_id": "order123"
        }
        result = send_order(trade)
        expected = simulate_order(trade)

        # Remove the 'timestamp' field from each event before comparison.
        result_no_ts = copy.deepcopy(result)
        expected_no_ts = copy.deepcopy(expected)
        for event in result_no_ts.get("events", []):
            event.pop("timestamp", None)
        for event in expected_no_ts.get("events", []):
            event.pop("timestamp", None)

        self.assertEqual(result_no_ts, expected_no_ts)


if __name__ == "__main__":
    unittest.main()