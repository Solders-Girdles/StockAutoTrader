#!/usr/bin/env python3
"""
reporting.py

Automated Risk Report Generation:
- Generates a daily (or intraday) risk summary report that includes:
    - Current portfolio exposures by asset class,
    - VaR trend over the past N days (rolling 3-day and 7-day trends simulated),
    - Summary of stress test outcomes and circuit breaker triggers.
- In production, this report can be published via email, Slack, or as a PDF snapshot.
"""

import json
from datetime import datetime, timedelta

def generate_daily_risk_report(portfolio: dict) -> dict:
    """
    Generate a daily risk report.

    Args:
        portfolio (dict): The current portfolio data.

    Returns:
        dict: A dictionary containing the risk report summary.
    """
    # Simulate exposures by asset class.
    # For demonstration, assume all positions are equity.
    exposures = {"equity": {}}
    for symbol, shares in portfolio.items():
        if symbol in ["total_capital", "available_margin", "daily_drawdown_pct"]:
            continue
        exposures["equity"][symbol] = shares

    # Simulate VaR trend for the past 7 days.
    today = datetime.utcnow().date()
    var_trend = []
    for i in range(7):
        day = today - timedelta(days=i)
        # Simplified: VaR is 2% of total capital with slight fluctuation.
        var_value = portfolio.get("total_capital", 0) * 0.02 * (1 + 0.005 * i)
        var_trend.append({"date": day.isoformat(), "var": var_value})
    var_trend = list(reversed(var_trend))  # oldest to newest

    # Count how many times the circuit breaker was triggered in the last week.
    # For simulation, assume we have a count.
    circuit_breaker_hits = 2  # This would be derived from historical logs in production.

    # Simulate stress test outcome.
    stress_test_outcome = "PASS" if portfolio.get("available_margin", 0) >= 0.2 * portfolio.get("total_capital", 0) else "FAIL"

    report = {
        "report_timestamp": datetime.utcnow().isoformat() + "Z",
        "portfolio_exposures_by_asset": exposures,
        "var_trend": var_trend,
        "stress_test_outcome": stress_test_outcome,
        "circuit_breaker_hits": circuit_breaker_hits,
        "daily_drawdown_pct": portfolio.get("daily_drawdown_pct", 0)
    }
    return report