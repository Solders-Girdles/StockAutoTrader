#!/usr/bin/env python3
"""
main.py

Main Entry Point for RiskOps Service:
- Receives trade signals (simulated for demonstration).
- Uses risk.py to perform risk checks, position sizing, and margin compliance.
- If approved, updates an in-memory portfolio and publishes the approved trade via mq_util.
- Logs every step with explicit JSON fields, including an optional correlation_id.
"""

import os
import json
import logging
from datetime import datetime, timezone

from risk import evaluate_trade
from mq_util import publish_message

# Configure consistent logging.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("RiskOpsMain")

# Simulated portfolio data for risk checks.
portfolio_data = {
    # Current position for symbols; for example, "AAPL": 100 means 100 shares held.
    "AAPL": 100,
    "available_margin": 50000.0  # Available margin in dollars.
}

def process_trade_signal(signal: dict, correlation_id: str = None) -> None:
    """
    Process a trade signal by evaluating risk, updating the portfolio, and
    publishing an approved trade if the risk check passes.

    Args:
        signal (dict): Trade signal with required keys.
        correlation_id (str): Optional correlation ID to trace the trade across services.
    """
    try:
        logger.info(json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "INFO",
            "service": "RiskOpsMain",
            "message": "Signal received.",
            "signal": signal,
            "correlation_id": correlation_id
        }))
        approved = evaluate_trade(signal, portfolio_data, correlation_id=correlation_id)
        if approved:
            if signal.get("action", "").upper() == "BUY":
                symbol = signal["symbol"]
                qty = int(signal.get("desired_quantity", 0))
                current = portfolio_data.get(symbol, 0)
                portfolio_data[symbol] = current + qty
                price = float(signal["price"])
                leverage_limit = 2.0
                margin_used = (price * qty) / leverage_limit
                portfolio_data["available_margin"] -= margin_used
                logger.info(json.dumps({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "level": "INFO",
                    "service": "RiskOpsMain",
                    "message": "Portfolio updated.",
                    "symbol": symbol,
                    "new_position": portfolio_data[symbol],
                    "available_margin": portfolio_data["available_margin"],
                    "correlation_id": correlation_id
                }))

            approved_trade_message = {
                "symbol": signal["symbol"],
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action": "EXECUTE_BUY" if signal.get("action", "").upper() == "BUY" else "EXECUTE_SELL",
                "price": signal["price"],
                "quantity": int(signal.get("desired_quantity", 50)),
                "risk_approved": True,
                "correlation_id": correlation_id
            }
            publish_message("approved_trades", approved_trade_message, correlation_id=correlation_id)
        else:
            logger.info(json.dumps({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": "INFO",
                "service": "RiskOpsMain",
                "message": "Trade signal rejected by risk evaluation.",
                "signal": signal,
                "correlation_id": correlation_id
            }))
    except Exception as e:
        logger.exception(json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "ERROR",
            "service": "RiskOpsMain",
            "message": "Error processing trade signal.",
            "error": str(e),
            "correlation_id": correlation_id
        }))

def main():
    # For demonstration, simulate consuming a trade signal.
    sample_signal = {
        "symbol": "AAPL",
        "timestamp": "2025-02-07T12:00:00Z",
        "action": "BUY",
        "price": 150.0,
        "desired_quantity": 50,
        "confidence": 0.80
    }
    # Optionally, a correlation_id can be generated or passed in.
    correlation_id = "abc123"
    process_trade_signal(sample_signal, correlation_id=correlation_id)

if __name__ == "__main__":
    main()