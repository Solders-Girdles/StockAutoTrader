# performance.py
import numpy as np
from datetime import datetime
from typing import Dict, Any, List
from logging_helper import log_json


class PerformanceTracker:
    """
    Tracks simulated trade performance and computes advanced metrics:
      - Annualized Sharpe Ratio and Sortino Ratio
      - Win/Loss Ratio (based on actual trade P&L)
      - Average Gain / Average Loss
      - Longest winning and losing streaks
    Uses simple trade pairing: a BUY opens a position; the next SELL closes it.
    """

    def __init__(self, initial_cash: float = 100000.0):
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.trades: List[Dict[str, Any]] = []  # Completed trades with profit details.
        self.cash_history: List[tuple] = []  # (timestamp, cash)
        self.open_position: Optional[Dict[str, Any]] = None  # Active BUY trade.

    def update(self, trade: Dict[str, Any]):
        action = trade.get("action")
        price = trade.get("price", 0)
        fill_ratio = trade.get("fill_ratio", 1.0)
        timestamp = trade.get("timestamp")

        if action == "BUY":
            if self.open_position is None:
                self.open_position = {
                    "symbol": trade.get("symbol"),
                    "buy_price": price,
                    "timestamp": timestamp
                }
        elif action == "SELL":
            if self.open_position is not None:
                buy_price = self.open_position.get("buy_price")
                profit = (price - buy_price) * fill_ratio
                trade["profit"] = profit
                self.cash += profit
                self.trades.append(trade)
                self.open_position = None
            else:
                self.cash += price * fill_ratio
                trade["profit"] = 0
                self.trades.append(trade)
        self.cash_history.append((timestamp, self.cash))

    def compute_metrics(self) -> Dict[str, Any]:
        # Compute daily returns.
        daily_returns = []
        days = {}
        for ts, cash in self.cash_history:
            day = ts[:10]
            days.setdefault(day, []).append(cash)
        for day, cash_list in days.items():
            if cash_list[0] != 0:
                ret = (cash_list[-1] - cash_list[0]) / cash_list[0]
            else:
                ret = 0
            daily_returns.append(ret)
        avg_return = np.mean(daily_returns) if daily_returns else 0
        std_return = np.std(daily_returns) if daily_returns else 1e-5
        sharpe_daily = avg_return / std_return if std_return != 0 else 0
        sharpe_annual = sharpe_daily * np.sqrt(252)

        downside_returns = [r for r in daily_returns if r < 0]
        downside_std = np.std(downside_returns) if downside_returns else 1e-5
        sortino_daily = avg_return / downside_std if downside_std != 0 else 0
        sortino_annual = sortino_daily * np.sqrt(252)

        # Win/Loss metrics based on actual profit.
        wins = [t for t in self.trades if t.get("profit", 0) > 0]
        losses = [t for t in self.trades if t.get("profit", 0) <= 0]
        win_loss_ratio = len(wins) / len(losses) if losses else float('inf')
        avg_gain = np.mean([t.get("profit", 0) for t in wins]) if wins else 0
        avg_loss = np.mean([t.get("profit", 0) for t in losses]) if losses else 0

        streak = 0
        longest_win_streak = 0
        longest_loss_streak = 0
        for t in self.trades:
            profit = t.get("profit", 0)
            if profit > 0:
                streak = streak + 1 if streak >= 0 else 1
            else:
                streak = streak - 1 if streak <= 0 else -1
            if streak > longest_win_streak:
                longest_win_streak = streak
            if streak < longest_loss_streak:
                longest_loss_streak = streak

        return {
            "sharpe_ratio": round(sharpe_annual, 2),
            "sortino_ratio": round(sortino_annual, 2),
            "win_loss_ratio": round(win_loss_ratio, 2),
            "avg_gain": round(avg_gain, 2),
            "avg_loss": round(avg_loss, 2),
            "longest_win_streak": longest_win_streak,
            "longest_loss_streak": abs(longest_loss_streak)
        }

    def report_performance(self) -> Dict[str, Any]:
        daily = {}
        for ts, cash in self.cash_history:
            day = ts[:10]
            daily.setdefault(day, []).append(cash)
        summary = {}
        for day, cash_values in daily.items():
            final_cash = cash_values[-1]
            pnl = final_cash - self.initial_cash
            trade_count = sum(1 for t in self.trades if t["timestamp"].startswith(day))
            summary[day] = {
                "final_cash": final_cash,
                "pnl": pnl,
                "trade_count": trade_count
            }
        metrics = self.compute_metrics()
        summary["advanced_metrics"] = metrics
        log_json("INFO", "Performance summary", extra={"performance": summary})
        return summary