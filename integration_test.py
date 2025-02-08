#!/usr/bin/env python3
"""
integration_test.py

This script simulates the full end-to-end trading workflow of the StockAutoTrader project.
It includes the following steps:
    1. Fetch Market Data
    2. Process Market Data and Compute Trade Signal
    3. Risk Evaluation and Portfolio Update
    4. Trade Execution
    5. Log Trade Execution and Report Summary

Features:
    - Structured JSON logging.
    - Simulated error conditions via command-line parameters.
    - Summary reporting.
    - Resource cleanup.
    - Parameterization via command-line arguments.
"""

import argparse
import logging
import json
import random
import uuid
import sys
from datetime import datetime

# Custom JSON Formatter for logging
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "time": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "message": record.getMessage()
        }
        if record.exc_info:
            log_record["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(log_record)

def setup_logger():
    """Set up and return a logger with JSON formatting."""
    logger = logging.getLogger("IntegrationTest")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)
    return logger

logger = setup_logger()

def fetch_market_data():
    """
    Simulate fetching market data.
    Returns a dictionary with keys: symbol, price, timestamp.
    """
    try:
        market_data = {
            "symbol": "AAPL",
            "price": round(random.uniform(140, 160), 2),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        logger.info(f"Market data fetched: {market_data}")
        return market_data
    except Exception:
        logger.error("Error fetching market data", exc_info=True)
        raise

def process_market_data(market_data):
    """
    Simulate processing the market data.
    Computes a trade signal with keys: symbol, timestamp, signal_type, price, and confidence.
    """
    try:
        signal_type = "BUY" if market_data["price"] < 150 else "SELL"
        confidence = round(random.uniform(0.5, 1.0), 2)
        trade_signal = {
            "symbol": market_data["symbol"],
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "signal_type": signal_type,
            "price": market_data["price"],
            "confidence": confidence
        }
        logger.info(f"Trade signal computed: {trade_signal}")
        return trade_signal
    except Exception:
        logger.error("Error processing market data", exc_info=True)
        raise

def risk_evaluation(trade_signal, risk_threshold, simulation_mode="none"):
    """
    Simulate risk evaluation and portfolio update.
    Approves the trade if trade_signal['confidence'] is above the risk_threshold.
    If simulation_mode is 'risk_failure', force a failure.
    Returns a tuple (approved: bool, message: str).
    """
    try:
        if simulation_mode == "risk_failure":
            approved = False
        else:
            approved = trade_signal["confidence"] > risk_threshold

        if approved:
            message = (f"Portfolio updated with {trade_signal['signal_type']} order for "
                       f"{trade_signal['symbol']} at {trade_signal['price']}")
            logger.info(f"Risk evaluation passed: {message}")
        else:
            message = (f"Trade signal not approved: Confidence {trade_signal['confidence']} "
                       f"below threshold {risk_threshold}.")
            logger.warning(f"Risk evaluation failed: {message}")
        return approved, message
    except Exception:
        logger.error("Error in risk evaluation", exc_info=True)
        raise

def execute_trade(trade_signal, simulation_mode="none"):
    """
    Simulate trade execution.
    Generates a unique trade ID, execution status, and execution time.
    If simulation_mode is 'execution_failure', simulate an execution error.
    """
    try:
        if simulation_mode == "execution_failure":
            raise Exception("Simulated trade execution failure.")
        trade_id = str(uuid.uuid4())
        execution_status = "SUCCESS"
        execution_time = datetime.utcnow().isoformat() + "Z"
        trade_execution = {
            "trade_id": trade_id,
            "status": execution_status,
            "execution_time": execution_time,
            "symbol": trade_signal["symbol"],
            "signal_type": trade_signal["signal_type"],
            "price": trade_signal["price"]
        }
        logger.info(f"Trade executed: {trade_execution}")
        return trade_execution
    except Exception:
        logger.error("Error executing trade", exc_info=True)
        raise

def cleanup_resources():
    """
    Simulate resource cleanup.
    """
    try:
        # Add cleanup logic here if necessary
        logger.info("Cleaning up resources.")
    except Exception:
        logger.error("Error during resource cleanup", exc_info=True)

def summarize_test(summary):
    """
    Print a summary report of the test run.
    """
    logger.info("Test Summary:")
    logger.info(json.dumps(summary, indent=2))

def main():
    """
    Main function to run the complete trading workflow.
    """
    parser = argparse.ArgumentParser(description="Integration test for StockAutoTrader")
    parser.add_argument("--risk-threshold", type=float, default=0.75,
                        help="Threshold for risk evaluation (default: 0.75)")
    parser.add_argument("--simulate", type=str, choices=["none", "risk_failure", "execution_failure"],
                        default="none", help="Simulation mode for error conditions")
    args = parser.parse_args()

    summary = {
        "market_data_fetched": False,
        "trade_signal_computed": False,
        "risk_evaluation": None,
        "trade_execution": None,
        "final_outcome": "Incomplete"
    }

    try:
        # Step 1: Fetch Market Data
        market_data = fetch_market_data()
        summary["market_data_fetched"] = True

        # Step 2: Process Market Data and Compute Trade Signal
        trade_signal = process_market_data(market_data)
        summary["trade_signal_computed"] = True

        # Step 3: Risk Evaluation and Portfolio Update
        approved, risk_message = risk_evaluation(trade_signal, args.risk_threshold, args.simulate)
        summary["risk_evaluation"] = {
            "approved": approved,
            "message": risk_message
        }
        if not approved:
            logger.error("Trade signal did not pass risk evaluation. Aborting trade execution.")
            summary["final_outcome"] = "Risk evaluation failed"
            return

        # Step 4: Trade Execution
        trade_execution = execute_trade(trade_signal, args.simulate)
        summary["trade_execution"] = trade_execution

        # Step 5: Log Trade Execution
        logger.info(f"Final trade execution details: {trade_execution}")
        summary["final_outcome"] = "Success"

    except Exception as e:
        logger.error("An error occurred in the trading workflow", exc_info=True)
        summary["final_outcome"] = f"Failed: {str(e)}"
    finally:
        cleanup_resources()
        summarize_test(summary)

if __name__ == "__main__":
    main()