#!/usr/bin/env python3
"""
backtester.py

This module implements a comprehensive backtesting framework for the Quant service.
It replays historical Polygon data from a CSV file (or DataFrame) while simulating:
  - Realistic transaction costs and slippage.
  - Partial fills and execution latencies.
  - Signal generation from our Quant strategy.

Additionally, a simple walk-forward parameter optimization routine is provided
to tune strategy parameters (e.g., MA periods) using rolling-window (walk-forward) testing.

Usage:
    python backtester.py --data historical_data.csv

Assumptions:
    - The historical data file is CSV with at least these columns:
        timestamp, symbol, price, volume
    - The strategy logic is encapsulated in our SymbolStrategy class (or similar).
    - Logs are output in JSON format with standard keys.
"""

import os
import json
import time
import logging
import random
import traceback
from datetime import datetime, timedelta
from collections import deque
from typing import Dict, Any, List, Optional

import pandas as pd
import numpy as np

# --- Logging Helper -----------------------------------------------------------
SERVICE_NAME = "Quant"


def log_json(level: str, message: str, extra: Optional[Dict[str, Any]] = None):
    """
    Log a JSON-formatted message with standardized fields.

    Standard keys:
      - timestamp: current UTC ISO8601 string.
      - level: log level.
      - service: service name.
      - message: log message.
      - extra: any additional key-value pairs (e.g., correlation_id).
    """
    if extra is None:
        extra = {}
    log_entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "level": level.upper(),
        "service": SERVICE_NAME,
        "message": message
    }
    log_entry.update(extra)
    if level.upper() == "ERROR":
        logging.error(json.dumps(log_entry))
    elif level.upper() == "WARNING":
        logging.warning(json.dumps(log_entry))
    elif level.upper() == "DEBUG":
        logging.debug(json.dumps(log_entry))
    else:
        logging.info(json.dumps(log_entry))


# Configure basic logging (all messages will be formatted as JSON via our helper).
logging.basicConfig(level=logging.INFO, format="%(message)s")


# --- Strategy Class (simplified version) -------------------------------------
class SymbolStrategy:
    """
    A simplified version of our strategy.
    Maintains a rolling window of prices and computes short and long moving averages,
    and (optionally) an RSI indicator. When a crossover occurs, a trade signal is generated.

    The generated signal includes both an "action" and "signal" key (and legacy "signal_type")
    for compatibility with RiskOps.
    """

    def __init__(self, symbol: str, ma_short: int, ma_long: int, rsi_period: int,
                 use_rsi: bool, rsi_buy: float, rsi_sell: float) -> None:
        self.symbol = symbol
        self.ma_short = ma_short
        self.ma_long = ma_long
        self.rsi_period = rsi_period
        self.use_rsi = use_rsi
        self.rsi_buy_threshold = rsi_buy
        self.rsi_sell_threshold = rsi_sell
        # Buffer size covers MA and RSI.
        self.buffer_size = max(ma_long, rsi_period + 1)
        self.prices = deque(maxlen=self.buffer_size)
        self.last_signal = ""

    def update(self, timestamp: str, price: float) -> Dict[str, Any]:
        """
        Update strategy with a new price, compute indicators, and generate a signal.

        Returns a dictionary with keys:
          - symbol, timestamp, price, ma_short, ma_long, rsi,
          - signal_type, action, signal, confidence.
        """
        self.prices.append(price)
        if len(self.prices) < self.ma_long:
            return {}  # Not enough data

        prices_list = list(self.prices)
        short_prices = prices_list[-self.ma_short:]
        long_prices = prices_list[-self.ma_long:]
        ma_short_value = sum(short_prices) / self.ma_short
        ma_long_value = sum(long_prices) / self.ma_long

        if ma_short_value > ma_long_value:
            base_signal = "BUY"
        elif ma_short_value < ma_long_value:
            base_signal = "SELL"
        else:
            base_signal = "HOLD"

        rsi_value = None
        if len(prices_list) >= self.rsi_period + 1:
            diffs = [prices_list[i] - prices_list[i - 1] for i in range(1, len(prices_list))]
            recent_diffs = diffs[-self.rsi_period:]
            gains = [d if d > 0 else 0 for d in recent_diffs]
            losses = [-d if d < 0 else 0 for d in recent_diffs]
            avg_gain = sum(gains) / self.rsi_period
            avg_loss = sum(losses) / self.rsi_period
            if avg_loss == 0:
                rsi_value = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi_value = 100 - (100 / (1 + rs))

        final_signal = base_signal
        if self.use_rsi and rsi_value is not None:
            if base_signal == "BUY" and rsi_value >= self.rsi_buy_threshold:
                final_signal = "HOLD"
            elif base_signal == "SELL" and rsi_value <= self.rsi_sell_threshold:
                final_signal = "HOLD"

        if final_signal != "HOLD" and final_signal != self.last_signal:
            self.last_signal = final_signal
            # Include both "action" and "signal" keys for RiskOps.
            return {
                "symbol": self.symbol,
                "timestamp": timestamp,
                "price": price,
                "ma_short": round(ma_short_value, 2),
                "ma_long": round(ma_long_value, 2),
                "rsi": round(rsi_value, 2) if rsi_value is not None else None,
                "signal_type": final_signal,
                "action": final_signal,
                "signal": final_signal,
                "confidence": 1.0  # For strategy-generated signals
            }
        return {}


# --- Backtester Class ---------------------------------------------------------
class Backtester:
    """
    Backtester for the Quant strategy.

    Parameters:
      - data: Historical data as a Pandas DataFrame.
      - strategy_cls: Class of the strategy to be tested.
      - tx_cost: Transaction cost rate (e.g., 0.1% as 0.001).
      - slippage_pct: Simulated slippage percentage (e.g., 0.1% as 0.001).
      - latency_sec: Simulated execution latency in seconds.
      - partial_fill: Ratio of order filled (e.g., 1.0 for full fill, <1.0 for partial).
    """

    def __init__(self,
                 data: pd.DataFrame,
                 strategy_cls,
                 ma_short: int,
                 ma_long: int,
                 rsi_period: int,
                 use_rsi: bool,
                 rsi_buy: float,
                 rsi_sell: float,
                 tx_cost: float = 0.001,
                 slippage_pct: float = 0.001,
                 latency_sec: float = 1.0,
                 partial_fill: float = 1.0,
                 initial_cash: float = 100000.0) -> None:
        self.data = data.sort_values("timestamp")
        self.strategy_cls = strategy_cls
        self.ma_short = ma_short
        self.ma_long = ma_long
        self.rsi_period = rsi_period
        self.use_rsi = use_rsi
        self.rsi_buy = rsi_buy
        self.rsi_sell = rsi_sell
        self.tx_cost = tx_cost
        self.slippage_pct = slippage_pct
        self.latency_sec = latency_sec
        self.partial_fill = partial_fill
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.trades: List[Dict[str, Any]] = []
        # Instantiate one strategy per symbol
        self.strategies: Dict[str, Any] = {}

    def simulate_trade(self, signal: Dict[str, Any], price: float) -> None:
        """
        Simulate the execution of a trade given a signal.

        Execution is delayed by latency_sec, price is adjusted by slippage_pct,
        and transaction costs are applied.
        """
        # Simulate latency (in a backtest we can simply add the latency to the timestamp)
        time.sleep(self.latency_sec)
        exec_price = price * (1 + self.slippage_pct if signal["action"] == "BUY" else 1 - self.slippage_pct)
        trade_value = exec_price * self.partial_fill
        cost = trade_value * self.tx_cost
        if signal["action"] == "BUY":
            self.cash -= (trade_value + cost)
        else:
            self.cash += (trade_value - cost)
        trade_record = {
            "timestamp": signal["timestamp"],
            "symbol": signal["symbol"],
            "action": signal["action"],
            "exec_price": exec_price,
            "fill_ratio": self.partial_fill,
            "tx_cost": cost,
            "cash": self.cash,
            "correlation_id": signal.get("correlation_id", None)
        }
        log_json("INFO", "Executed trade", extra=trade_record)
        self.trades.append(trade_record)

    def run(self) -> Dict[str, Any]:
        """
        Run the backtest over the historical data.
        Returns performance metrics and trade logs.
        """
        for idx, row in self.data.iterrows():
            ts = row["timestamp"]
            symbol = row["symbol"]
            price = row["price"]
            # Initialize strategy for the symbol if not already done.
            if symbol not in self.strategies:
                self.strategies[symbol] = self.strategy_cls(symbol,
                                                            self.ma_short,
                                                            self.ma_long,
                                                            self.rsi_period,
                                                            self.use_rsi,
                                                            self.rsi_buy,
                                                            self.rsi_sell)
            strategy = self.strategies[symbol]
            signal = strategy.update(ts, price)
            if not signal:
                # Fallback: we could also call a compute_signal() function here.
                continue
            if signal["action"] != "HOLD":
                # For this backtest, assume the signal triggers immediate execution.
                self.simulate_trade(signal, price)
        performance = {
            "initial_cash": self.initial_cash,
            "final_cash": self.cash,
            "profit": self.cash - self.initial_cash,
            "number_of_trades": len(self.trades)
        }
        log_json("INFO", "Backtest complete", extra=performance)
        return performance


# --- Parameter Optimization (Walk-Forward Testing) ---------------------------
def walk_forward_optimization(data: pd.DataFrame,
                              strategy_cls,
                              param_grid: Dict[str, List[Any]],
                              window_size: int = 252,  # e.g., training window (days)
                              test_size: int = 63) -> List[Dict[str, Any]]:
    """
    Perform walk-forward optimization using rolling windows.

    For each window, optimize parameters (e.g., MA periods) based on training data,
    then test on the subsequent out-of-sample period.

    Args:
        data: Historical data DataFrame.
        strategy_cls: The strategy class to optimize.
        param_grid: Dictionary where keys are parameter names and values are lists of candidate values.
        window_size: Number of data points for training.
        test_size: Number of data points for testing.

    Returns:
        A list of dictionaries, each with the optimized parameters and test performance.
    """
    results = []
    total_points = len(data)
    # Ensure data is sorted by timestamp.
    data = data.sort_values("timestamp").reset_index(drop=True)
    start = 0
    while start + window_size + test_size <= total_points:
        train_data = data.iloc[start:start + window_size]
        test_data = data.iloc[start + window_size:start + window_size + test_size]
        best_perf = -np.inf
        best_params = {}
        # Grid search over candidate parameters.
        for ma_short in param_grid.get("ma_short", [2]):
            for ma_long in param_grid.get("ma_long", [5]):
                # For each candidate, run a backtest on the training window.
                bt = Backtester(train_data, strategy_cls, ma_short, ma_long,
                                param_grid.get("rsi_period", [14])[0],
                                param_grid.get("use_rsi", [False])[0],
                                param_grid.get("rsi_buy", [30])[0],
                                param_grid.get("rsi_sell", [70])[0])
                perf = bt.run()["profit"]
                if perf > best_perf:
                    best_perf = perf
                    best_params = {"ma_short": ma_short, "ma_long": ma_long}
        # Run test with best_params
        bt_test = Backtester(test_data, strategy_cls,
                             best_params["ma_short"],
                             best_params["ma_long"],
                             param_grid.get("rsi_period", [14])[0],
                             param_grid.get("use_rsi", [False])[0],
                             param_grid.get("rsi_buy", [30])[0],
                             param_grid.get("rsi_sell", [70])[0])
        test_perf = bt_test.run()
        result = {
            "train_start": train_data.iloc[0]["timestamp"],
            "train_end": train_data.iloc[-1]["timestamp"],
            "test_start": test_data.iloc[0]["timestamp"],
            "test_end": test_data.iloc[-1]["timestamp"],
            "optimized_params": best_params,
            "train_profit": best_perf,
            "test_profit": test_perf["profit"]
        }
        log_json("INFO", "Walk-forward optimization window complete", extra=result)
        results.append(result)
        start += test_size  # roll forward by test period
    return results


# --- Main Block ---------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Quant Backtester")
    parser.add_argument("--data", type=str, required=True,
                        help="Path to CSV file containing historical data (timestamp,symbol,price,volume)")
    parser.add_argument("--optimize", action="store_true", help="Run walk-forward optimization")
    args = parser.parse_args()

    try:
        hist_data = pd.read_csv(args.data)
        # Ensure timestamp is in proper datetime format (ISO8601 strings expected)
        hist_data["timestamp"] = pd.to_datetime(hist_data["timestamp"]).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        log_json("ERROR", "Failed to load historical data", extra={"exception": str(e)})
        exit(1)

    if args.optimize:
        # Example parameter grid for optimization.
        param_grid = {
            "ma_short": [2, 3, 4],
            "ma_long": [5, 7, 10],
            "rsi_period": [14],
            "use_rsi": [False],
            "rsi_buy": [30],
            "rsi_sell": [70]
        }
        opt_results = walk_forward_optimization(hist_data, SymbolStrategy, param_grid)
        log_json("INFO", "Optimization complete", extra={"results": opt_results})
    else:
        # Run a simple backtest with default parameters.
        bt = Backtester(hist_data, SymbolStrategy, MA_SHORT, MA_LONG, RSI_PERIOD,
                        USE_RSI_CONFIRMATION, RSI_BUY_THRESHOLD, RSI_SELL_THRESHOLD)
        performance = bt.run()
        log_json("INFO", "Backtest performance", extra=performance)