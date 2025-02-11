#!/usr/bin/env python3
"""
broker_api.py

This module handles connectivity to the broker/exchange API and provides a function to send orders
with retry logic, circuit-breaker and timeout wrapping, and standardized JSON logging.

Real Broker Sandbox Integration (Example: Alpaca Paper Trading API)
--------------------------------------------------------------------
Authentication:
  - API credentials are provided via environment variables: ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL.
  - For Alpaca, the base URL is typically "https://paper-api.alpaca.markets".

Order Submission Endpoint:
  - Orders are submitted via the Alpaca REST API (using the alpaca_trade_api library).
  - Example endpoint: POST /v2/orders
  - Order parameters include symbol, quantity, side, order type (here "limit"), limit_price, and time_in_force ("gtc").

Rate Limits / Throttling:
  - Refer to the broker’s documentation. Our retry logic with exponential backoff (2, 4, 8 seconds)
    and a simple circuit breaker (threshold 3, recovery timeout 30 seconds) help avoid overwhelming the API.

Advanced Order Types:
  - For now, we support basic limit orders. In simulation mode, if trade.get("order_type") is provided,
    additional logic can be added to mimic stop-loss or bracket orders.
"""

import os
import json
import time
import logging
import traceback
import uuid
from datetime import datetime, timezone

# Attempt to import the Alpaca API; if unavailable, fall back to simulation.
try:
    import alpaca_trade_api as tradeapi
except ImportError:
    tradeapi = None

from pika.exceptions import AMQPConnectionError, AMQPChannelError

logger = logging.getLogger("ExecConnect.BrokerAPI")
logger.setLevel(logging.INFO)

ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY")
ALPACA_BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")


# --- Simple Circuit Breaker Implementation ---
class CircuitBreaker:
    def __init__(self, failure_threshold=3, recovery_timeout=30):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # Possible states: "CLOSED", "OPEN", "HALF-OPEN"

    def call(self, func, *args, **kwargs):
        if self.state == "OPEN":
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = "HALF-OPEN"
            else:
                raise Exception("Circuit breaker is OPEN")
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
            raise e
        else:
            if self.state == "HALF-OPEN":
                self.reset()
            return result

    def reset(self):
        self.failure_count = 0
        self.state = "CLOSED"
        self.last_failure_time = None


circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=30)

# Timeout for broker API calls (in seconds)
BROKER_API_TIMEOUT = 10


def _send_order_api_call(trade: dict) -> dict:
    """
    Internal function to send an order to the broker API.
    This function is wrapped by the circuit breaker and timeout logic.
    """
    api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url=ALPACA_BASE_URL)
    order = api.submit_order(
        symbol=trade['symbol'],
        qty=trade['quantity'],
        side=trade['action'].lower(),
        type='limit',  # For now, we support limit orders; advanced order types can be added later.
        limit_price=trade['price'],
        time_in_force='gtc'
    )
    execution_result = {
        "order_id": order.id,
        "status": order.status.upper(),
        "filled_quantity": int(order.filled_qty),
        "avg_price": float(order.filled_avg_price) if order.filled_avg_price else trade['price'],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "remaining_quantity": trade['quantity'] - int(order.filled_qty),
        "order_state": order.status.upper()
    }
    return execution_result


def send_order(trade: dict, max_retries: int = 3) -> dict:
    """
    Sends an order to the broker API using Alpaca (if configured) with timeout and circuit breaker logic.
    Includes retry logic for transient connectivity issues.

    Parameters:
        trade (dict): A dictionary containing trade details.
        max_retries (int): Maximum number of retry attempts (default is 3).

    Returns:
        dict: Execution result with order details.
             For simulation of partial fills, a dict with an "events" key is returned.
    """
    attempt = 1
    while attempt <= max_retries:
        try:
            log_msg = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": "INFO",
                "service": "ExecConnect.BrokerAPI",
                "message": f"Sending order, attempt {attempt}",
                "trade": trade,
                "correlation_id": trade.get("correlation_id")
            }
            logger.info(json.dumps(log_msg))

            # If the broker API is not configured, fall back to simulation.
            if not tradeapi or not ALPACA_API_KEY:
                return simulate_order(trade)

            # Wrap the API call with circuit breaker and timeout.
            from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(lambda: circuit_breaker.call(_send_order_api_call, trade))
                result = future.result(timeout=BROKER_API_TIMEOUT)

            log_msg = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": "INFO",
                "service": "ExecConnect.BrokerAPI",
                "message": "Order sent successfully",
                "execution_result": result,
                "correlation_id": trade.get("correlation_id")
            }
            logger.info(json.dumps(log_msg))
            return result

        except FuturesTimeoutError:
            delay = 2 ** attempt
            log_msg = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": "ERROR",
                "service": "ExecConnect.BrokerAPI",
                "message": f"Broker API call timed out on attempt {attempt}",
                "retry_delay": delay,
                "correlation_id": trade.get("correlation_id")
            }
            logger.error(json.dumps(log_msg))
            time.sleep(delay)
            attempt += 1

        except (AMQPConnectionError, AMQPChannelError) as e:
            delay = 2 ** attempt
            log_msg = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": "ERROR",
                "service": "ExecConnect.BrokerAPI",
                "message": f"Broker connectivity error on attempt {attempt}: {str(e)}",
                "stack_trace": traceback.format_exc(),
                "retry_delay": delay,
                "correlation_id": trade.get("correlation_id")
            }
            logger.error(json.dumps(log_msg))
            time.sleep(delay)
            attempt += 1

        except Exception as e:
            log_msg = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": "ERROR",
                "service": "ExecConnect.BrokerAPI",
                "message": f"Unexpected error sending order: {str(e)}",
                "stack_trace": traceback.format_exc(),
                "correlation_id": trade.get("correlation_id")
            }
            logger.error(json.dumps(log_msg))
            raise

    default_result = {
        "order_id": "N/A",
        "status": "FAILED",
        "filled_quantity": 0,
        "avg_price": 0.0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "remaining_quantity": trade.get("quantity", 0),
        "order_state": "FAILED"
    }
    log_msg = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": "ERROR",
        "service": "ExecConnect.BrokerAPI",
        "message": "All broker API retries failed",
        "default_result": default_result,
        "correlation_id": trade.get("correlation_id")
    }
    logger.error(json.dumps(log_msg))
    return default_result


def simulate_order(trade: dict) -> dict:
    """
    Simulate an order execution when the broker API is not configured.
    Supports simulation of full and partial fills based on the 'scenario' key.

    Parameters:
        trade (dict): A dictionary containing trade details.

    Returns:
        dict: For full fill scenarios, returns a single event inside a dict with key "events".
              For partial fill scenarios (when trade['scenario'] == "partial"),
              returns a dict with key "events" containing a list of execution events.
    """
    scenario = trade.get("scenario", "full").lower()
    risk_approved = trade.get("risk_approved", False)
    order_id = trade.get("order_id", f"SIM-{uuid.uuid4()}")
    base_timestamp = datetime.now(timezone.utc).isoformat()

    # Advanced Order Types simulation (for future extension):
    if trade.get("order_type"):
        # Additional logic for advanced order types (e.g., stop-loss, OCO) would go here.
        pass

    if scenario == "partial":
        if trade["quantity"] > 1:
            first_fill = int(trade["quantity"] * 0.6)  # simulate 60% fill
        else:
            first_fill = trade["quantity"]
        first_event = {
            "order_id": order_id,
            "status": "PARTIALLY_FILLED",
            "filled_quantity": first_fill,
            "avg_price": trade["price"] + 0.05,  # slight slippage
            "timestamp": base_timestamp,
            "remaining_quantity": trade["quantity"] - first_fill,
            "order_state": "PARTIALLY_FILLED"
        }
        final_timestamp = datetime.now(timezone.utc).isoformat()
        final_event = {
            "order_id": order_id,
            "status": "FILLED",
            "filled_quantity": trade["quantity"],
            "avg_price": trade["price"],
            "timestamp": final_timestamp,
            "remaining_quantity": 0,
            "order_state": "FILLED"
        }
        return {"events": [first_event, final_event]}
    else:
        status = "FILLED" if risk_approved else "REJECTED"
        event = {
            "order_id": order_id,
            "status": status,
            "filled_quantity": trade["quantity"] if status == "FILLED" else 0,
            "avg_price": trade["price"],
            "timestamp": base_timestamp,
            "remaining_quantity": 0 if status == "FILLED" else trade.get("quantity", 0),
            "order_state": status
        }
        return {"events": [event]}