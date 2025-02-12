# quant_tests/strategies.py
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


