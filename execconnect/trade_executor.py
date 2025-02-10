#!/usr/bin/env python3
"""
trade_executor.py

This module contains the core logic for executing trades by interfacing with the broker API.
It validates trade requests, calls the broker API (or its simulation), and logs execution details.
It now also logs an initial PENDING event and returns a list of execution events.
"""

import json
import logging
import traceback
import uuid
from datetime import datetime
from typing import Dict, List

from broker_api import send_order

logger = logging.getLogger("ExecConnect.TradeExecutor")
logger.setLevel(logging.INFO)


def execute_trade(trade: Dict) -> List[Dict]:
    """
    Executes a trade by sending an order via the broker API.
    Logs an initial "PENDING" state event, then sends the order and returns a list of execution events.
    Each event (including partial fill events) includes order_id, status, filled_quantity, avg_price,
    timestamp, remaining_quantity, and order_state.

    Parameters:
        trade (dict): A dictionary with keys such as 'symbol', 'action', 'price', 'quantity',
                      'risk_approved', and optionally 'correlation_id' for traceability.

    Returns:
        List[Dict]: A list of execution event dictionaries, including an initial "PENDING" event.
    """
    # Ensure a consistent order_id across events.
    if "order_id" not in trade:
        trade["order_id"] = str(uuid.uuid4())

    pending_event = {
        "order_id": trade["order_id"],
        "status": "PENDING",
        "filled_quantity": 0,
        "avg_price": 0.0,
        "timestamp": datetime.utcnow().isoformat(),
        "remaining_quantity": trade.get("quantity"),
        "order_state": "PENDING"
    }
    logger.info(json.dumps({
        "timestamp": datetime.utcnow().isoformat(),
        "level": "INFO",
        "service": "ExecConnect.TradeExecutor",
        "message": "Trade pending",
        "execution_event": pending_event,
        "correlation_id": trade.get("correlation_id")
    }))

    try:
        result = send_order(trade)
        # The send_order function returns a dict with a key "events" (a list of events).
        events = result.get("events")
        # Ensure all events use the same order_id.
        for event in events:
            event["order_id"] = trade["order_id"]
        logger.info(json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "level": "INFO",
            "service": "ExecConnect.TradeExecutor",
            "message": "Trade execution events",
            "execution_events": events,
            "correlation_id": trade.get("correlation_id")
        }))
        return [pending_event] + events
    except Exception as e:
        log_msg = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": "ERROR",
            "service": "ExecConnect.TradeExecutor",
            "message": f"Error executing trade: {str(e)}",
            "stack_trace": traceback.format_exc(),
            "correlation_id": trade.get("correlation_id")
        }
        logger.error(json.dumps(log_msg))
        raise


def log_trade_execution(execution_event: Dict) -> None:
    """
    Logs a single trade execution event.
    In production, this might write to a database; here we use structured JSON logging.

    Parameters:
        execution_event (dict): The trade execution event to log.
    """
    try:
        logger.info(json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "level": "INFO",
            "service": "ExecConnect.TradeExecutor",
            "message": "Logging trade execution event",
            "execution_event": execution_event
        }))
    except Exception as e:
        logger.error(json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "level": "ERROR",
            "service": "ExecConnect.TradeExecutor",
            "message": f"Error logging trade execution event: {str(e)}",
            "stack_trace": traceback.format_exc()
        }))