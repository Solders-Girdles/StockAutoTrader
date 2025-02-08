#!/usr/bin/env python3
"""
main.py

Main Entry Point for RiskOps Service:
- Receives trade signals (simulated for demonstration).
- Uses risk.py to perform comprehensive risk checks (including multi-asset risk, exposure limits, circuit breakers, VaR, and stress tests).
- If approved, updates an in-memory portfolio and publishes the approved trade via mq_util.
- Generates an automated daily risk report via reporting.py.
- Logs every step with explicit JSON fields (timestamp, level, service, message, correlation_id).
"""

import os
import json
import logging
from datetime import datetime, timezone

from risk import evaluate_trade
from mq_util import publish_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("RiskOpsMain")

# Simulated portfolio data for risk checks.
portfolio_data = {
    "AAPL": 100,                # current shares held for AAPL
    "total_capital": 100000.0,    # total portfolio capital
    "available_margin": 50000.0,  # available margin in dollars
    "daily_drawdown_pct": 0.02    # current daily drawdown (2% loss)
}

def process_trade_signal(signal: dict, correlation_id: str = None) -> None:
    """
    Process a trade signal by evaluating risk, updating the portfolio, and publishing an approved trade if approved.

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
                asset_class = signal.get("asset_class", "equity").lower()
                asset_params = {
                    "equity": {"leverage_limit": 2.0},
                    "options": {"leverage_limit": 1.5},
                    "futures": {"leverage_limit": 5.0},
                    "crypto": {"leverage_limit": 2.5}
                }
                leverage_limit = asset_params.get(asset_class, asset_params["equity"])["leverage_limit"]
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
    # For demonstration, simulate consuming a multi-asset trade signal.
    sample_signal = {
        "symbol": "AAPL",
        "timestamp": "2025-02-07T12:00:00Z",
        "action": "BUY",
        "price": 150.0,
        "desired_quantity": 50,
        "confidence": 0.80,
        "asset_class": "equity",
        "vix": 25  # Normal volatility; if >30, would be rejected.
    }
    correlation_id = "abc123"
    process_trade_signal(sample_signal, correlation_id=correlation_id)

    # Generate and log a daily risk report.
    from reporting import generate_daily_risk_report
    report = generate_daily_risk_report(portfolio_data)
    logger.info(json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": "INFO",
        "service": "RiskOpsMain",
        "message": "Daily risk report generated.",
        "report": report,
        "correlation_id": correlation_id
    }))

if __name__ == "__main__":
    main()