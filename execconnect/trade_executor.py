#!/usr/bin/env python3
"""
trade_executor.py

This module contains the core logic for executing trades by interfacing with the broker API.
It validates trade requests, calls the broker API (or a simulation), and logs execution details.
"""

import json
import logging
import traceback
from datetime import datetime
from typing import Dict

from broker_api import send_order

logger = logging.getLogger("ExecConnect.TradeExecutor")
logger.setLevel(logging.INFO)


def execute_trade(trade: Dict) -> Dict:
    """
    Executes a trade by sending an order via the broker API.
    Logs the attempt and result using structured JSON logging.

    Parameters:
        trade (dict): A dictionary with keys such as 'symbol', 'action', 'price', 'quantity',
                      'risk_approved', and optionally 'correlation_id' for traceability.

    Returns:
        dict: Execution details returned from the broker API or simulation.
    """
    try:
        log_msg = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": "INFO",
            "service": "ExecConnect.TradeExecutor",
            "message": "Executing trade",
            "trade": trade,
            "correlation_id": trade.get("correlation_id")
        }
        logger.info(json.dumps(log_msg))

        result = send_order(trade)

        log_msg = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": "INFO",
            "service": "ExecConnect.TradeExecutor",
            "message": "Trade execution result",
            "execution_result": result,
            "correlation_id": trade.get("correlation_id")
        }
        logger.info(json.dumps(log_msg))
        return result

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


def log_trade_execution(execution_result: Dict) -> None:
    """
    Logs the trade execution details.
    In production, this might write to a database.
    Here we use structured JSON logging.

    Parameters:
        execution_result (dict): The trade execution details to log.
    """
    try:
        log_msg = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": "INFO",
            "service": "ExecConnect.TradeExecutor",
            "message": "Logging trade execution",
            "execution_result": execution_result
        }
        logger.info(json.dumps(log_msg))
    except Exception as e:
        log_msg = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": "ERROR",
            "service": "ExecConnect.TradeExecutor",
            "message": f"Error logging trade execution: {str(e)}",
            "stack_trace": traceback.format_exc()
        }
        logger.error(json.dumps(log_msg))