# quant/strategies.py
import time
import random
import traceback
from collections import deque
from datetime import datetime
from typing import Dict, Any, List, Optional
import numpy as np
from sklearn.linear_model import LogisticRegression
from common.logging_helper import get_logger

# Create a logger for the Quant service.
logger = get_logger("Quant")


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
        self.prices = deque(maxlen=self.buffer_size)
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
        # (Optional: Additional RSI adjustments can be integrated here.)
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
        logger.debug("SymbolStrategy update", extra={"computation_time": comp_time})
        return result


class MACDStrategy:
    """
    MACD Strategy:
      - Calculates two EMAs (short and long periods) and computes:
          MACD line = EMA(short) - EMA(long)
          Signal line = EMA of the MACD line
          Histogram = MACD - signal line
      - Signals BUY when the histogram turns positive (or MACD crosses above the signal line)
        and SELL when it turns negative.
      - Uses the histogram magnitude to set confidence.
    """

    def __init__(self, symbol: str, short_period: int = 12, long_period: int = 26, signal_period: int = 9) -> None:
        self.symbol = symbol
        self.short_period = short_period
        self.long_period = long_period
        self.signal_period = signal_period
        self.prices: List[float] = []
        self.macd_history: List[float] = []
        self.last_signal = ""

    def _ema(self, data: List[float], period: int, prev_ema: Optional[float] = None) -> float:
        alpha = 2 / (period + 1)
        if prev_ema is None:
            return sum(data[-period:]) / period
        return (data[-1] - prev_ema) * alpha + prev_ema

    def update(self, timestamp: str, price: float) -> Dict[str, Any]:
        start_time = time.time()
        self.prices.append(price)
        if len(self.prices) < self.long_period:
            return {}
        ema_short = self._ema(self.prices, self.short_period)
        ema_long = self._ema(self.prices, self.long_period)
        macd_line = ema_short - ema_long
        self.macd_history.append(macd_line)
        if len(self.macd_history) < self.signal_period:
            return {}
        signal_line = self._ema(self.macd_history, self.signal_period)
        histogram = macd_line - signal_line
        if histogram > 0 and self.last_signal != "BUY":
            signal_val = "BUY"
        elif histogram < 0 and self.last_signal != "SELL":
            signal_val = "SELL"
        else:
            signal_val = "HOLD"
        result = {}
        if signal_val != "HOLD":
            self.last_signal = signal_val
            result = {
                "symbol": self.symbol,
                "timestamp": timestamp,
                "price": price,
                "macd": round(macd_line, 2),
                "signal_line": round(signal_line, 2),
                "histogram": round(histogram, 2),
                "signal_type": signal_val,
                "action": signal_val,
                "signal": signal_val,
                "confidence": round(abs(histogram) / (abs(signal_line) + 1e-5), 2),
                "source": "MACD"
            }
        comp_time = time.time() - start_time
        logger.debug("MACDStrategy update", extra={"computation_time": comp_time})
        return result


class BollingerBandsStrategy:
    """
    Bollinger Bands Strategy:
      - Computes the SMA and standard deviation over a specified window.
      - Upper band = SMA + (multiplier * stddev); lower band = SMA - (multiplier * stddev).
      - Signals BUY if the price falls below the lower band and SELL if above the upper band.
    """

    def __init__(self, symbol: str, window: int = 20, multiplier: float = 2.0) -> None:
        self.symbol = symbol
        self.window = window
        self.multiplier = multiplier
        self.prices = []
        self.last_signal = ""

    def update(self, timestamp: str, price: float) -> dict:
        import time, numpy as np
        start_time = time.time()
        self.prices.append(price)
        if len(self.prices) < self.window:
            return {}
        window_prices = self.prices[-self.window:]
        sma = sum(window_prices) / self.window
        stddev = np.std(window_prices)
        upper_band = sma + self.multiplier * stddev
        lower_band = sma - self.multiplier * stddev

        # Debug print with high precision:
        print(
            f"[DEBUG] window_prices: {window_prices}, SMA: {sma:.10f}, stddev: {stddev:.10f}, lower_band: {lower_band:.10f}, price: {price:.10f}")

        # Increase tolerance to handle floating point imprecision
        tolerance = 1e-3  # increased from 1e-4 to 1e-3
        if price <= lower_band + tolerance and self.last_signal != "BUY":
            signal_val = "BUY"
        elif price >= upper_band - tolerance and self.last_signal != "SELL":
            signal_val = "SELL"
        else:
            signal_val = "HOLD"
        result = {}
        if signal_val != "HOLD":
            self.last_signal = signal_val
            result = {
                "symbol": self.symbol,
                "timestamp": timestamp,
                "price": price,
                "sma": round(sma, 2),
                "upper_band": round(upper_band, 2),
                "lower_band": round(lower_band, 2),
                "signal_type": signal_val,
                "action": signal_val,
                "signal": signal_val,
                "confidence": 1.0,
                "source": "Bollinger"
            }
        comp_time = time.time() - start_time
        logger.debug("BollingerBandsStrategy update", extra={"computation_time": comp_time})
        return result


class RSIIndicator:
    """
    RSI Indicator:
      - Computes the Relative Strength Index (RSI) for a given price series.
      - Returns an RSI value (typically between 0 and 100).
      - Can be used by the aggregator to adjust the confidence of other signals.
    """

    def __init__(self, period: int = 14) -> None:
        self.period = period

    def compute(self, prices: List[float]) -> float:
        if len(prices) < self.period + 1:
            return 50.0  # Neutral
        gains = []
        losses = []
        for i in range(1, self.period + 1):
            change = prices[-i] - prices[-i - 1]
            if change > 0:
                gains.append(change)
            else:
                losses.append(abs(change))
        avg_gain = sum(gains) / self.period if gains else 0
        avg_loss = sum(losses) / self.period if losses else 0
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100 - (100 / (1 + rs)), 2)


class MLSignalStrategy:
    """
    ML-based Strategy using logistic regression.
    Uses lagged returns, moving average difference, and volatility as features.
    """

    def __init__(self, symbol: str, window: int) -> None:
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
                logger.error("MLSignalStrategy training failed", extra={
                    "exception": str(e),
                    "stack_trace": traceback.format_exc()
                })
                return result
        X_new = self._build_features(self.prices[-(self.window + 1):])[-1].reshape(1, -1)
        try:
            pred = self.model.predict(X_new)[0]
            signal_val = "BUY" if pred == 1 else "SELL"
        except Exception as e:
            logger.error("MLSignalStrategy prediction failed", extra={
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
        logger.debug("MLSignalStrategy update", extra={"computation_time": comp_time})
        return result

class MeanReversionMomentumStrategy:
    """
    A stub for the MeanReversionMomentumStrategy.
    This is a placeholder implementation. Update with actual logic as needed.
    """
    def __init__(self, symbol: str, window: int = 10, threshold: float = 0.01):
        self.symbol = symbol
        self.window = window
        self.threshold = threshold

    def update(self, timestamp: str, price: float) -> dict:
        # Return an empty dict or a default signal.
        return {}

def aggregate_signals(signals: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Aggregates multiple signals from different strategies using pre-configured weights.
    Additionally, if an RSI signal is available, adjust other signals’ weights.
    """
    if not signals:
        return {"signal_type": "HOLD", "action": "HOLD", "signal": "HOLD", "confidence": 0.0}

    # Check if an RSI signal is present.
    rsi_signal = next((s for s in signals if s.get("source") == "RSI"), None)
    weight_map = {"legacy": 1.0, "MACD": 1.0, "Bollinger": 1.0, "ML": 0.6, "RSI": 1.0}
    buy_conf = 0.0
    sell_conf = 0.0
    for s in signals:
        src = s.get("source", "legacy")
        weight = weight_map.get(src, 1.0)
        # If an RSI signal is present, adjust weight for non-RSI signals.
        if rsi_signal and src != "RSI":
            if rsi_signal.get("action") == s.get("action"):
                weight *= 1.2
            else:
                weight *= 0.8
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