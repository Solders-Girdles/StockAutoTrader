#!/usr/bin/env python3
"""
monitoring.py

Real-Time Risk Monitoring Dashboard:
- Exposes an HTTP endpoint (/metrics) that returns current risk metrics in JSON format.
- Metrics include:
  - Current portfolio exposures (symbol-level),
  - Active VaR estimate,
  - Circuit breaker status,
  - Historical drawdowns.
- Designed for near real-time monitoring by Grafana or a custom UI.
"""

from flask import Flask, jsonify
import logging
from datetime import datetime

app = Flask(__name__)
logger = logging.getLogger("Monitoring")
logger.setLevel(logging.INFO)

# For demonstration, use a global portfolio and historical drawdowns.
portfolio_data = {
    "AAPL": 150,                # Example: 150 shares held for AAPL
    "total_capital": 100000.0,
    "available_margin": 40000.0,
    "daily_drawdown_pct": 0.03    # 3% drawdown
}

# Simulated historical drawdowns.
historical_drawdowns = [
    {"date": "2025-02-05", "drawdown_pct": 0.02},
    {"date": "2025-02-06", "drawdown_pct": 0.04},
]

def calculate_var(portfolio):
    # Simplified VaR: 2% of total capital.
    return portfolio.get("total_capital", 0) * 0.02

@app.route("/metrics", methods=["GET"])
def metrics():
    var_estimate = calculate_var(portfolio_data)
    metrics = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "portfolio_exposures": portfolio_data,
        "active_var": var_estimate,
        "circuit_breaker_triggered": portfolio_data.get("daily_drawdown_pct", 0) >= 0.05,
        "historical_drawdowns": historical_drawdowns
    }
    return jsonify(metrics)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)