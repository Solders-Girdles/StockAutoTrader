#!/usr/bin/env python3
"""
risk.py

Core Risk Logic Module:
- Contains functions to evaluate trade risk based on position sizing,
  margin requirements, and signal confidence.
- Logs risk thresholds, violations, and resolution steps using explicit JSON fields.
"""

import time
import logging
from datetime import datetime, timezone

# Configure a logger for the risk module.
logger = logging.getLogger("Risk")
logger.setLevel(logging.INFO)

def _json_log(level: str, service: str, message: str, extra: dict = None):
    log = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "service": service,
        "message": message
    }
    if extra:
        log.update(extra)
    return log

def evaluate_trade(signal: dict, portfolio: dict,
                   max_position: int = 1000,
                   leverage_limit: float = 2.0,
                   confidence_threshold: float = 0.75,
                   correlation_id: str = None) -> bool:
    """
    Evaluate the trade risk based on position size, margin compliance,
    and signal confidence.

    Args:
        signal (dict): Trade signal with keys:
            - 'symbol' (str)
            - 'action' (str): "BUY" or "SELL"
            - 'price' (number)
            - 'desired_quantity' (int)
            - 'confidence' (float)
        portfolio (dict): Contains current positions and available margin.
            Example:
                {
                  "AAPL": 100,          # current position for symbol
                  "available_margin": 50000.0
                }
        max_position (int): Maximum allowed position size per symbol.
        leverage_limit (float): Leverage limit (e.g., 2.0 means margin requirement is half the notional).
        confidence_threshold (float): Minimum signal confidence required.
        correlation_id (str): Optional correlation identifier for logging.

    Returns:
        bool: True if trade is approved; False otherwise.
    """
    try:
        symbol = signal["symbol"]
        action = signal["action"].upper()
        price = float(signal["price"])
        desired_qty = int(signal.get("desired_quantity", 0))
        confidence = float(signal.get("confidence", 1.0))
    except Exception as e:
        logger.exception(_json_log(
            level="ERROR",
            service="RiskOps",
            message="Risk evaluation error: missing or invalid field in signal.",
            extra={"error": str(e), "signal": signal, "correlation_id": correlation_id}
        ))
        return False

    # Check signal confidence.
    if confidence < confidence_threshold:
        logger.info(_json_log(
            level="INFO",
            service="RiskOps",
            message="Risk rejected due to low confidence.",
            extra={"confidence": confidence, "threshold": confidence_threshold, "symbol": symbol, "correlation_id": correlation_id}
        ))
        return False

    if action == "BUY":
        current_position = portfolio.get(symbol, 0)
        new_position = current_position + desired_qty
        if new_position > max_position:
            logger.warning(_json_log(
                level="WARNING",
                service="RiskOps",
                message="Risk rejected: Over position limit.",
                extra={"symbol": symbol, "current": current_position, "desired": desired_qty, "max": max_position, "correlation_id": correlation_id}
            ))
            return False

        margin_required = (price * desired_qty) / leverage_limit
        available_margin = portfolio.get("available_margin", 0)
        if margin_required > available_margin:
            logger.warning(_json_log(
                level="WARNING",
                service="RiskOps",
                message="Risk rejected: Insufficient margin.",
                extra={"symbol": symbol, "margin_required": margin_required, "available_margin": available_margin, "correlation_id": correlation_id}
            ))
            return False

    # For SELL orders or other actions, additional checks can be added as needed.
    logger.info(_json_log(
        level="INFO",
        service="RiskOps",
        message="Risk approved.",
        extra={"symbol": symbol, "action": action, "desired_quantity": desired_qty, "correlation_id": correlation_id}
    ))
    return True