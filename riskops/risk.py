#!/usr/bin/env python3
"""
risk.py

Core Risk Logic Module:
- Evaluates trade risk based on position sizing, margin requirements, exposure limits,
  circuit breakers, VaR, stress tests, and multi-asset parameters.
- Supports extended scenario codes:
    * "low_confidence"
    * "circuit_breaker"
    * "over_position_limit"
    * "exposure_limit_breach"
    * "stress_test_fail"
    * "extreme_volatility"
    * "under_margin_call"
- Logs all decisions with explicit JSON fields.
"""

import logging
from datetime import datetime, timezone

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
                   circuit_breaker_pct: float = 0.05,  # reject if daily drawdown ≥5%
                   var_pct: float = 0.02,              # simplified VaR = 2% of total capital
                   stress_drop_pct: float = 0.10,      # simulate a 10% market drop
                   stress_threshold_pct: float = 0.05,   # stress exposure must not exceed 5% of capital
                   confidence_threshold: float = 0.75,
                   correlation_id: str = None) -> bool:
    """
    Evaluate trade risk based on multiple factors and annotate decisions with a reason code.

    Args:
        signal (dict): Trade signal containing:
            - 'symbol': str
            - 'action': str ("BUY" or "SELL")
            - 'price': numeric
            - 'desired_quantity': int
            - 'confidence': float
            - 'asset_class': (optional) str; e.g., "equity", "options", "futures", "crypto"
            - 'vix': (optional) numeric; for volatility checks.
        portfolio (dict): Contains current positions, 'total_capital', 'available_margin',
                          and 'daily_drawdown_pct'.
        correlation_id (str): Optional correlation ID for logging.

    Returns:
        bool: True if the trade is approved; False otherwise.
    """
    # Determine asset class and set asset-specific parameters.
    asset_class = signal.get("asset_class", "equity").lower()
    asset_params = {
        "equity": {"leverage_limit": 2.0, "max_exposure_pct": 0.10},
        "options": {"leverage_limit": 1.5, "max_exposure_pct": 0.15},
        "futures": {"leverage_limit": 5.0, "max_exposure_pct": 0.20},
        "crypto": {"leverage_limit": 2.5, "max_exposure_pct": 0.10}
    }
    params = asset_params.get(asset_class, asset_params["equity"])
    leverage_limit = params["leverage_limit"]
    max_exposure_pct = params["max_exposure_pct"]

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
            message="Risk evaluation error: missing/invalid field in signal.",
            extra={"error": str(e), "signal": signal, "correlation_id": correlation_id, "reason_code": "invalid_signal"}
        ))
        return False

    # Extended Scenario: Check for extreme volatility via VIX.
    if "vix" in signal:
        try:
            vix = float(signal["vix"])
            if vix > 30:  # threshold for extreme volatility
                logger.warning(_json_log(
                    level="WARNING",
                    service="RiskOps",
                    message="Risk rejected: Extreme volatility.",
                    extra={"vix": vix, "threshold": 30, "correlation_id": correlation_id, "reason_code": "extreme_volatility"}
                ))
                return False
        except Exception:
            pass  # Ignore if VIX is malformed

    # Check signal confidence.
    if confidence < confidence_threshold:
        logger.info(_json_log(
            level="INFO",
            service="RiskOps",
            message="Risk rejected due to low confidence.",
            extra={"confidence": confidence, "threshold": confidence_threshold, "symbol": symbol, "correlation_id": correlation_id, "reason_code": "low_confidence"}
        ))
        return False

    # Check circuit breaker: if daily drawdown exceeds threshold.
    if portfolio.get("daily_drawdown_pct", 0) >= circuit_breaker_pct:
        logger.warning(_json_log(
            level="WARNING",
            service="RiskOps",
            message="Risk rejected: Circuit breaker triggered.",
            extra={"daily_drawdown_pct": portfolio.get("daily_drawdown_pct"),
                   "circuit_breaker_pct": circuit_breaker_pct, "correlation_id": correlation_id, "reason_code": "circuit_breaker"}
        ))
        return False

    # Extended Scenario: Under margin call if available margin is very low.
    if portfolio.get("available_margin", 0) < (0.1 * portfolio.get("total_capital", 0)):
        logger.warning(_json_log(
            level="WARNING",
            service="RiskOps",
            message="Risk rejected: Under margin call.",
            extra={"available_margin": portfolio.get("available_margin"),
                   "total_capital": portfolio.get("total_capital"),
                   "correlation_id": correlation_id, "reason_code": "under_margin_call"}
        ))
        return False

    if action == "BUY":
        # Check per-symbol position limit.
        current_position = portfolio.get(symbol, 0)
        new_position = current_position + desired_qty
        if new_position > max_position:
            logger.warning(_json_log(
                level="WARNING",
                service="RiskOps",
                message="Risk rejected: Exceeds maximum position size.",
                extra={"symbol": symbol, "current": current_position, "desired": desired_qty, "max_position": max_position, "correlation_id": correlation_id, "reason_code": "over_position_limit"}
            ))
            return False

        # Check per-symbol exposure limit.
        new_exposure = new_position * price
        max_exposure = portfolio["total_capital"] * max_exposure_pct
        if new_exposure > max_exposure:
            logger.warning(_json_log(
                level="WARNING",
                service="RiskOps",
                message="Risk rejected: Exposure exceeds allowed percentage.",
                extra={"symbol": symbol, "new_exposure": new_exposure, "max_exposure": max_exposure, "correlation_id": correlation_id, "reason_code": "exposure_limit_breach"}
            ))
            return False

        # Log VaR estimate.
        calculated_var = portfolio["total_capital"] * var_pct
        logger.info(_json_log(
            level="INFO",
            service="RiskOps",
            message="Calculated daily VaR.",
            extra={"calculated_var": calculated_var, "correlation_id": correlation_id}
        ))

        # Stress test: simulate a market drop.
        stress_exposure = new_exposure * stress_drop_pct
        if stress_exposure > portfolio["total_capital"] * stress_threshold_pct:
            logger.warning(_json_log(
                level="WARNING",
                service="RiskOps",
                message="Risk rejected: Stress test failed; post-stress exposure too high.",
                extra={"symbol": symbol, "stress_exposure": stress_exposure,
                       "allowed": portfolio["total_capital"] * stress_threshold_pct,
                       "correlation_id": correlation_id, "reason_code": "stress_test_fail"}
            ))
            return False

    # For SELL orders (or additional actions), extra checks can be added here.
    logger.info(_json_log(
        level="INFO",
        service="RiskOps",
        message="Risk approved.",
        extra={"symbol": symbol, "action": action, "desired_quantity": desired_qty, "asset_class": asset_class, "correlation_id": correlation_id, "reason_code": "approved"}
    ))
    return True