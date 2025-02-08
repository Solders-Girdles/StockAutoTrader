#!/usr/bin/env python3
"""
quant/main.py

A robust multi-strategy Quant service that consumes market data from RabbitMQ,
validates the data, and generates trading signals using one or more strategy modules.
Signals include keys "action", "signal", and "signal_type" for RiskOps compatibility,
as well as additional metadata such as "confidence" and a correlation_id (if provided).

New features include:
  1. PerformanceTracker: Tracks daily performance metrics (P&L, trade count, max drawdown).
  2. Cross-Strategy Aggregation: When STRATEGY_TYPE=AGGREGATED, signals from multiple strategies
     are combined using a weighting scheme.
  3. Expanded ML features: MLSignalStrategy now includes volatility as a feature.
  4. Paper-trade feedback: Partial fill data (fill_ratio) is incorporated into P&L calculations.

Environment Variables:
    STRATEGY_TYPE: "SYMBOL" (default), "MRM", "ML", or "AGGREGATED"
    -- Strategy Parameters (common) --
    MA_SHORT, MA_LONG, RSI_PERIOD, USE_RSI_CONFIRMATION, RSI_BUY_THRESHOLD, RSI_SELL_THRESHOLD
    ML_WINDOW: (for MLSignalStrategy)
    -- RabbitMQ Connection --
    RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS
    -- Optional Postgres Logging --
    DB_HOST, DB_USER, DB_PASS, DB_NAME
"""

import os
import sys
import json
import time
import logging
import random
import traceback
from datetime import datetime, timedelta
from collections import deque
from typing import Dict, Any, Deque, Optional, List

import pika
import psycopg2

# For ML strategy:
from sklearn.linear_model import LogisticRegression
import numpy as np

# -----------------------------------------------------
# Logging Helper
# -----------------------------------------------------
SERVICE_NAME = "Quant"


def log_json(level: str, message: str, extra: Optional[Dict[str, Any]] = None):
    """
    Log a JSON-formatted message with standard keys: timestamp, level, service, and message.
    Optionally, extra fields (e.g., correlation_id) are added.
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


logging.basicConfig(level=logging.INFO, format="%(message)s")

# -----------------------------------------------------
# Environment & Parameters
# -----------------------------------------------------
STRATEGY_TYPE = os.environ.get("STRATEGY_TYPE", "SYMBOL").upper()
MA_SHORT = int(os.environ.get("MA_SHORT", "2"))
MA_LONG = int(os.environ.get("MA_LONG", "5"))
RSI_PERIOD = int(os.environ.get("RSI_PERIOD", "14"))
USE_RSI_CONFIRMATION = os.environ.get("USE_RSI_CONFIRMATION", "false").lower() in ("true", "1", "yes")
RSI_BUY_THRESHOLD = float(os.environ.get("RSI_BUY_THRESHOLD", "30"))
RSI_SELL_THRESHOLD = float(os.environ.get("RSI_SELL_THRESHOLD", "70"))
ML_WINDOW = int(os.environ.get("ML_WINDOW", "50"))

# Postgres (optional)
DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")


# -----------------------------------------------------
# Core Strategy Classes
# -----------------------------------------------------
class SymbolStrategy:
    """
    Legacy moving-average crossover strategy.
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
        self.buffer_size = max(ma_long, rsi_period + 1)
        self.prices: Deque[float] = deque(maxlen=self.buffer_size)
        self.last_signal = ""

    def update(self, timestamp: str, price: float) -> Dict[str, Any]:
        start_time = time.time()
        self.prices.append(price)
        result: Dict[str, Any] = {}
        if len(self.prices) < self.ma_long:
            return result
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
        final_signal = base_signal
        if self.use_rsi:
            # (RSI logic can be added here.)
            pass
        if final_signal != "HOLD" and final_signal != self.last_signal:
            self.last_signal = final_signal
            result = {
                "symbol": self.symbol,
                "timestamp": timestamp,
                "price": price,
                "signal_type": final_signal,
                "action": final_signal,
                "signal": final_signal,
                "confidence": 1.0,
                "source": "legacy"
            }
        comp_time = time.time() - start_time
        log_json("DEBUG", "SymbolStrategy update", extra={"computation_time": comp_time})
        return result


class MeanReversionMomentumStrategy:
    """
    Strategy that combines mean reversion and momentum.
    """

    def __init__(self, symbol: str, window: int = 10, threshold: float = 0.01) -> None:
        self.symbol = symbol
        self.window = window
        self.threshold = threshold
        self.prices: Deque[float] = deque(maxlen=window)
        self.last_signal = ""

    def update(self, timestamp: str, price: float) -> Dict[str, Any]:
        start_time = time.time()
        self.prices.append(price)
        if len(self.prices) < self.window:
            return {}
        prices_list = list(self.prices)
        ma = sum(prices_list) / self.window
        momentum = price - prices_list[-2] if len(prices_list) >= 2 else 0.0
        if price < ma * (1 - self.threshold) and momentum > 0:
            signal_val = "BUY"
        elif price > ma * (1 + self.threshold) and momentum < 0:
            signal_val = "SELL"
        else:
            signal_val = "HOLD"
        result = {}
        if signal_val != "HOLD" and signal_val != self.last_signal:
            self.last_signal = signal_val
            result = {
                "symbol": self.symbol,
                "timestamp": timestamp,
                "price": price,
                "ma": round(ma, 2),
                "momentum": round(momentum, 2),
                "signal_type": signal_val,
                "action": signal_val,
                "signal": signal_val,
                "confidence": 0.9,
                "source": "MRM"
            }
        comp_time = time.time() - start_time
        log_json("DEBUG", "MeanReversionMomentumStrategy update", extra={"computation_time": comp_time})
        return result


class MLSignalStrategy:
    """
    A simple ML-based strategy using logistic regression.
    Expanded to include volatility as an additional feature.
    """

    def __init__(self, symbol: str, window: int = ML_WINDOW) -> None:
        self.symbol = symbol
        self.window = window
        self.prices: List[float] = []
        self.model: Optional[LogisticRegression] = None
        self.last_signal = ""

    def _build_features(self, prices: List[float]) -> np.ndarray:
        X = []
        for i in range(1, len(prices)):
            ret = (prices[i] - prices[i - 1]) / prices[i - 1]
            ma = np.mean(prices[max(0, i - 5):i])
            diff = (prices[i] - ma) / ma if ma != 0 else 0
            if i >= 5:
                returns = [(prices[j] - prices[j - 1]) / prices[j - 1] for j in range(i - 4, i + 1)]
                vol = np.std(returns)
            else:
                vol = 0
            X.append([ret, diff, vol])
        return np.array(X)

    def _build_labels(self, prices: List[float]) -> np.ndarray:
        y = []
        for i in range(len(prices) - 1):
            y.append(1 if prices[i + 1] > prices[i] else 0)
        return np.array(y)

    def train_model(self):
        if len(self.prices) < self.window + 1:
            return
        X = self._build_features(self.prices[-(self.window + 1):])
        y = self._build_labels(self.prices[-(self.window + 1):])
        self.model = LogisticRegression()
        self.model.fit(X, y)

    def update(self, timestamp: str, price: float) -> Dict[str, Any]:
        start_time = time.time()
        self.prices.append(price)
        result: Dict[str, Any] = {}
        if len(self.prices) < self.window + 1:
            return result
        if self.model is None or len(self.prices) % self.window == 0:
            try:
                self.train_model()
            except Exception as e:
                log_json("ERROR", "MLSignalStrategy training failed", extra={
                    "exception": str(e),
                    "stack_trace": traceback.format_exc()
                })
                return result
        X_new = self._build_features(self.prices[-(self.window + 1):])[-1].reshape(1, -1)
        try:
            pred = self.model.predict(X_new)[0]
            signal_val = "BUY" if pred == 1 else "SELL"
        except Exception as e:
            log_json("ERROR", "MLSignalStrategy prediction failed", extra={
                "exception": str(e),
                "stack_trace": traceback.format_exc()
            })
            return result
        if signal_val != self.last_signal:
            self.last_signal = signal_val
            result = {
                "symbol": self.symbol,
                "timestamp": timestamp,
                "price": price,
                "signal_type": signal_val,
                "action": signal_val,
                "signal": signal_val,
                "confidence": round(random.uniform(0.75, 0.95), 2),
                "source": "ML"
            }
        comp_time = time.time() - start_time
        log_json("DEBUG", "MLSignalStrategy update", extra={"computation_time": comp_time})
        return result


# -----------------------------------------------------
# Aggregator & Performance Tracker
# -----------------------------------------------------
def aggregate_signals(signals: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Aggregate multiple signals from different strategies using weighting.
    Weight mapping: legacy=1.0, MRM=0.4, ML=0.6.
    Returns a consolidated signal.
    """
    if not signals:
        return {"signal_type": "HOLD", "action": "HOLD", "signal": "HOLD", "confidence": 0.0}
    weight_map = {"legacy": 1.0, "MRM": 0.4, "ML": 0.6}
    buy_conf = 0.0
    sell_conf = 0.0
    for s in signals:
        src = s.get("source", "legacy")
        weight = weight_map.get(src, 1.0)
        conf = s.get("confidence", 0) * weight
        if s.get("action") == "BUY":
            buy_conf += conf
        elif s.get("action") == "SELL":
            sell_conf += conf
    if buy_conf == 0 and sell_conf == 0:
        return {"signal_type": "HOLD", "action": "HOLD", "signal": "HOLD", "confidence": 0.0}
    if buy_conf > sell_conf:
        action = "BUY"
        agg_conf = buy_conf / (buy_conf + sell_conf)
    elif sell_conf > buy_conf:
        action = "SELL"
        agg_conf = sell_conf / (buy_conf + sell_conf)
    else:
        action = "HOLD"
        agg_conf = 0.0
    return {
        "symbol": signals[0].get("symbol", "UNKNOWN"),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "signal_type": action,
        "action": action,
        "signal": action,
        "confidence": round(agg_conf, 2)
    }


class PerformanceTracker:
    """
    Tracks simulated trade performance.
    Records each trade (incorporating fill_ratio for partial fills) and cash history,
    then reports daily metrics: final cash, P&L, number of trades, and max drawdown.
    """

    def __init__(self, initial_cash: float = 100000.0):
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.trades: List[Dict[str, Any]] = []
        self.cash_history: List[tuple] = []  # (timestamp, cash)

    def update(self, trade: Dict[str, Any]):
        fill_ratio = trade.get("fill_ratio", 1.0)
        price = trade.get("price", 0)
        if trade.get("action") == "BUY":
            self.cash -= price * fill_ratio
        elif trade.get("action") == "SELL":
            self.cash += price * fill_ratio
        trade["fill_ratio"] = fill_ratio
        self.trades.append(trade)
        self.cash_history.append((trade["timestamp"], self.cash))

    def report_performance(self) -> Dict[str, Any]:
        # Group cash history by day.
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
        # Compute max drawdown over entire history.
        cash_series = [cash for ts, cash in self.cash_history]
        peak = -float('inf')
        max_dd = 0
        for cash in cash_series:
            if cash > peak:
                peak = cash
            dd = peak - cash
            if dd > max_dd:
                max_dd = dd
        summary["max_drawdown"] = max_dd
        log_json("INFO", "Performance summary", extra={"performance": summary})
        return summary


# -----------------------------------------------------
# Data Validation & Helper Functions
# -----------------------------------------------------
def process_market_data(data: dict) -> dict:
    if "symbol" not in data or not data.get("symbol") or not str(data["symbol"]).strip():
        err = {"error": "Missing or empty required field: 'symbol'"}
        log_json("ERROR", "Data validation error", extra=err)
        raise ValueError(err)
    if "price" not in data:
        err = {"error": "Missing required field: 'price'"}
        log_json("ERROR", "Data validation error", extra=err)
        raise ValueError(err)
    try:
        symbol = str(data["symbol"]).strip()
        price = float(data["price"])
    except (ValueError, TypeError) as e:
        log_json("ERROR", "Invalid type for 'symbol' or 'price'", extra={
            "exception": str(e),
            "stack_trace": traceback.format_exc()
        })
        raise
    timestamp = data.get("timestamp") or datetime.utcnow().isoformat() + "Z"
    volume = data.get("volume")
    if volume is None:
        volume = 0
        log_json("WARNING", "Missing volume; defaulting to 0")
    else:
        try:
            volume = float(volume)
        except (ValueError, TypeError) as e:
            log_json("ERROR", "Invalid type for 'volume'", extra={
                "exception": str(e),
                "stack_trace": traceback.format_exc()
            })
            volume = 0
    processed_data = {
        "symbol": symbol,
        "timestamp": timestamp,
        "price": price,
        "volume": volume
    }
    log_json("INFO", "Processed market data", extra={"data": processed_data})
    return processed_data


def compute_signal(data: dict) -> dict:
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            price = data["price"]
            signal_val = "BUY" if price < 125 else "SELL"
            signal = {
                "symbol": data["symbol"],
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "signal_type": signal_val,
                "action": signal_val,
                "signal": signal_val,
                "price": price,
                "confidence": round(random.uniform(0.7, 1.0), 2),
                "source": "fallback"
            }
            log_json("INFO", "Computed fallback signal", extra={"signal": signal})
            return signal
        except KeyError as ke:
            log_json("ERROR", "Missing key during signal computation", extra={
                "exception": str(ke),
                "stack_trace": traceback.format_exc(),
                "attempt": attempt
            })
            break
        except Exception as e:
            log_json("ERROR", "Error computing signal", extra={
                "exception": str(e),
                "stack_trace": traceback.format_exc(),
                "attempt": attempt
            })
            time.sleep(0.5 * attempt)
    fallback_signal = {
        "symbol": data.get("symbol", "UNKNOWN"),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "signal_type": "HOLD",
        "action": "HOLD",
        "signal": "HOLD",
        "price": data.get("price", 0.0),
        "confidence": 0.0,
        "source": "fallback"
    }
    log_json("INFO", "Returning fallback signal", extra={"signal": fallback_signal})
    return fallback_signal


def publish_trade_signal(signal: dict) -> None:
    rabbitmq_host = os.environ.get("RABBITMQ_HOST", "localhost")
    rabbitmq_user = os.environ.get("RABBITMQ_USER", "guest")
    rabbitmq_pass = os.environ.get("RABBITMQ_PASS", "guest")
    message = json.dumps(signal)
    max_retries = 3
    delay = 1
    for attempt in range(1, max_retries + 1):
        try:
            credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
            parameters = pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials)
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue="trade_signals", durable=True)
            channel.basic_publish(
                exchange="",
                routing_key="trade_signals",
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            log_json("INFO", "Published trade signal", extra={"signal": signal})
            connection.close()
            return
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
            log_json("ERROR", "RabbitMQ network error while publishing signal", extra={
                "exception": str(e),
                "stack_trace": traceback.format_exc(),
                "attempt": attempt
            })
        except Exception as e:
            log_json("ERROR", "Unexpected error while publishing trade signal", extra={
                "exception": str(e),
                "stack_trace": traceback.format_exc(),
                "attempt": attempt
            })
        time.sleep(delay)
        delay *= 2
    log_json("ERROR", "Failed to publish trade signal after retries", extra={"signal": signal})


# -----------------------------------------------------
# Optional Postgres Logging
# -----------------------------------------------------
class PostgresLogger:
    def __init__(self, host: str, user: str, password: str, dbname: str) -> None:
        self.conn = psycopg2.connect(host=host, user=user, password=password, dbname=dbname)
        self.create_table()

    def create_table(self) -> None:
        query = """
        CREATE TABLE IF NOT EXISTS quant_signals (
            id SERIAL PRIMARY KEY,
            time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            symbol TEXT NOT NULL,
            action TEXT NOT NULL,
            ma_short NUMERIC,
            ma_long NUMERIC,
            rsi NUMERIC
        );
        """
        cur = self.conn.cursor()
        cur.execute(query)
        self.conn.commit()
        cur.close()

    def log_signal(self, signal: Dict[str, Any]) -> None:
        query = """
        INSERT INTO quant_signals (time, symbol, action, ma_short, ma_long, rsi)
        VALUES (NOW(), %s, %s, %s, %s, %s);
        """
        cur = self.conn.cursor()
        cur.execute(query, (
            signal.get("symbol"),
            signal.get("action"),
            signal.get("ma_short"),
            signal.get("ma_long"),
            signal.get("rsi")
        ))
        self.conn.commit()
        cur.close()

    def close(self) -> None:
        self.conn.close()


def get_rabbitmq_connection() -> pika.BlockingConnection:
    rabbitmq_host = os.environ.get("RABBITMQ_HOST", "localhost")
    rabbitmq_user = os.environ.get("RABBITMQ_USER", "guest")
    rabbitmq_pass = os.environ.get("RABBITMQ_PASS", "guest")
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
    parameters = pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials)
    return pika.BlockingConnection(parameters)


# -----------------------------------------------------
# Main Consumer Logic
# -----------------------------------------------------
def main() -> None:
    log_json("INFO", "Starting Quant multi-strategy service...")
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue="market_data", durable=True)
    channel.queue_declare(queue="trade_signals", durable=True)

    postgres_logger: Optional[PostgresLogger] = None
    if DB_HOST and DB_USER and DB_PASS and DB_NAME:
        try:
            postgres_logger = PostgresLogger(DB_HOST, DB_USER, DB_PASS, DB_NAME)
            log_json("INFO", "Connected to Postgres for signal logging.")
        except Exception:
            log_json("ERROR", "Failed to connect to Postgres; continuing without DB logging",
                     extra={"stack_trace": traceback.format_exc()})

    # Initialize performance tracker
    performance_tracker = PerformanceTracker(initial_cash=100000.0)

    # Prepare strategy instances.
    # If STRATEGY_TYPE == "AGGREGATED", run all three strategies per symbol.
    strategies: Dict[str, Any] = {}

    def on_message(ch, method, properties, body) -> None:
        try:
            raw_data = json.loads(body)
            processed_data = process_market_data(raw_data)
            symbol = processed_data["symbol"]
            timestamp = processed_data["timestamp"]
            price = processed_data["price"]
            signals: List[Dict[str, Any]] = []
            if STRATEGY_TYPE == "AGGREGATED":
                if symbol not in strategies:
                    strategies[symbol] = {
                        "legacy": SymbolStrategy(symbol, MA_SHORT, MA_LONG, RSI_PERIOD,
                                                 USE_RSI_CONFIRMATION, RSI_BUY_THRESHOLD, RSI_SELL_THRESHOLD),
                        "MRM": MeanReversionMomentumStrategy(symbol, window=10, threshold=0.01),
                        "ML": MLSignalStrategy(symbol, window=ML_WINDOW)
                    }
                for strat in strategies[symbol].values():
                    s = strat.update(timestamp, price)
                    if s:
                        signals.append(s)
                aggregated_signal = aggregate_signals(signals)
                final_signal = aggregated_signal
            else:
                if symbol not in strategies:
                    if STRATEGY_TYPE == "MRM":
                        strategies[symbol] = MeanReversionMomentumStrategy(symbol, window=10, threshold=0.01)
                    elif STRATEGY_TYPE == "ML":
                        strategies[symbol] = MLSignalStrategy(symbol, window=ML_WINDOW)
                    else:
                        strategies[symbol] = SymbolStrategy(symbol, MA_SHORT, MA_LONG, RSI_PERIOD,
                                                            USE_RSI_CONFIRMATION, RSI_BUY_THRESHOLD, RSI_SELL_THRESHOLD)
                final_signal = strategies[symbol].update(timestamp, price)
                if not final_signal:
                    final_signal = compute_signal(processed_data)
            # Incorporate partial fill data from ExecConnect.
            if "fill_ratio" not in final_signal:
                final_signal["fill_ratio"] = raw_data.get("fill_ratio", 1.0)
            if final_signal and final_signal.get("signal_type") != "HOLD":
                publish_trade_signal(final_signal)
                log_json("INFO", "Published signal", extra={"signal": final_signal})
                if postgres_logger:
                    try:
                        postgres_logger.log_signal(final_signal)
                        log_json("DEBUG", "Signal logged to Postgres")
                    except Exception:
                        log_json("ERROR", "Failed to log signal to Postgres",
                                 extra={"stack_trace": traceback.format_exc()})
                performance_tracker.update(final_signal)
        except Exception:
            log_json("ERROR", "Error processing market data message", extra={"stack_trace": traceback.format_exc()})
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue="market_data", on_message_callback=on_message)
    log_json("INFO", "Waiting for market data. To exit press CTRL+C")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log_json("INFO", "Interrupted by user; shutting down...")
    finally:
        channel.stop_consuming()
        connection.close()
        if postgres_logger:
            postgres_logger.close()
        # Report performance metrics (e.g., daily summary)
        performance_tracker.report_performance()
        log_json("INFO", "Shutdown complete.")


if __name__ == "__main__":
    main()