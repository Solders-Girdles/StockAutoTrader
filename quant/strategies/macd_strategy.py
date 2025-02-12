from dataclasses import dataclass
from typing import Dict, Any, List, Optional, NamedTuple
import time
from enum import Enum
import pandas as pd
import numpy as np


class SignalType(Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass
class MACDParameters:
    short_period: int = 12
    long_period: int = 26
    signal_period: int = 9


class MACDResult(NamedTuple):
    macd_line: float
    signal_line: float
    histogram: float


class MACDStrategy:
    """
    MACD (Moving Average Convergence Divergence) Strategy Implementation.

    This class implements the MACD technical indicator with configurable parameters
    for short EMA, long EMA, and signal line periods. It provides trading signals
    based on MACD histogram crossovers.
    """

    def __init__(
            self,
            symbol: str,
            params: MACDParameters = MACDParameters(),
            price_buffer_size: int = 100
    ) -> None:
        self.symbol = symbol
        self.params = params
        self.price_buffer_size = max(price_buffer_size, params.long_period * 2)
        self.prices: List[float] = []
        self.macd_history: List[float] = []
        self.last_signal = SignalType.HOLD
        self.last_ema_short: Optional[float] = None
        self.last_ema_long: Optional[float] = None
        self.last_signal_line: Optional[float] = None

        # Initialize logger
        from common.logging_helper import get_logger
        self.logger = get_logger(f"{self.__class__.__name__}_{symbol}")

    def _ema(self, data: List[float], period: int, prev_ema: Optional[float] = None) -> float:
        """Calculate Exponential Moving Average efficiently."""
        alpha = 2 / (period + 1)
        if prev_ema is None:
            return pd.Series(data[-period:]).ewm(span=period, adjust=False).mean().iloc[-1]
        return data[-1] * alpha + prev_ema * (1 - alpha)

    def _calculate_macd(self, price: float) -> MACDResult:
        """Calculate MACD components using cached EMAs for efficiency."""
        self.last_ema_short = self._ema(
            self.prices, self.params.short_period, self.last_ema_short
        )
        self.last_ema_long = self._ema(
            self.prices, self.params.long_period, self.last_ema_long
        )
        macd_line = self.last_ema_short - self.last_ema_long
        self.macd_history.append(macd_line)

        # Maintain buffer size for MACD history
        if len(self.macd_history) > self.price_buffer_size:
            self.macd_history = self.macd_history[-self.price_buffer_size:]

        self.last_signal_line = self._ema(
            self.macd_history, self.params.signal_period, self.last_signal_line
        )
        return MACDResult(
            macd_line=macd_line,
            signal_line=self.last_signal_line,
            histogram=macd_line - self.last_signal_line
        )

    def _generate_signal(self, histogram: float) -> SignalType:
        """Generate trading signal based on histogram value and previous signal."""
        if histogram > 0 and self.last_signal != SignalType.BUY:
            return SignalType.BUY
        elif histogram < 0 and self.last_signal != SignalType.SELL:
            return SignalType.SELL
        return SignalType.HOLD

    def update(self, timestamp: str, price: float) -> Dict[str, Any]:
        """
        Update the MACD strategy with new price data and generate trading signals.

        Args:
            timestamp: Current timestamp
            price: Current price

        Returns:
            Dictionary containing trading signal and MACD information if a signal is generated
        """
        start_time = time.time()

        try:
            self.prices.append(price)

            # Maintain price buffer size
            if len(self.prices) > self.price_buffer_size:
                self.prices = self.prices[-self.price_buffer_size:]

            if len(self.prices) < self.params.long_period:
                return {}

            macd_result = self._calculate_macd(price)

            if len(self.macd_history) < self.params.signal_period:
                return {}

            signal = self._generate_signal(macd_result.histogram)

            if signal == SignalType.HOLD:
                return {}

            self.last_signal = signal
            confidence = abs(macd_result.histogram) / (abs(macd_result.signal_line) + 1e-5)

            result = {
                "symbol": self.symbol,
                "timestamp": timestamp,
                "price": price,
                "macd": round(macd_result.macd_line, 2),
                "signal_line": round(macd_result.signal_line, 2),
                "histogram": round(macd_result.histogram, 2),
                "signal_type": signal.value,
                "action": signal.value,
                "signal": signal.value,
                "confidence": round(confidence, 2),
                "source": "MACD"
            }

            return result

        except Exception as e:
            self.logger.error(
                "Error in MACD update",
                extra={
                    "error": str(e),
                    "symbol": self.symbol,
                    "price": price,
                    "timestamp": timestamp
                }
            )
            raise

        finally:
            comp_time = time.time() - start_time
            self.logger.debug(
                "MACDStrategy update completed",
                extra={
                    "computation_time": comp_time,
                    "symbol": self.symbol,
                    "prices_buffer_length": len(self.prices)
                }
            )