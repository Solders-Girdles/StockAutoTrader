from dataclasses import dataclass
from collections import deque
from enum import Enum
from typing import Dict, Any, Optional, Deque
import time
from quant.indicators.rsi_indicator import RSIIndicator


class SignalType(Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class SignalSource(Enum):
    RSI = "RSI"
    MA = "MA"


@dataclass
class StrategyParameters:
    ma_short: int
    ma_long: int
    rsi_period: int
    use_rsi: bool = True
    rsi_buy_threshold: float = 30.0
    rsi_sell_threshold: float = 70.0


class SymbolStrategy:
    """
    Combined Moving Average Crossover and RSI Strategy.

    This strategy implements two technical analysis approaches:
    1. RSI (Relative Strength Index) for overbought/oversold conditions
    2. Moving Average Crossover for trend following

    RSI signals are prioritized over Moving Average signals when both are enabled.
    """

    def __init__(self, symbol: str, params: StrategyParameters) -> None:
        """
        Initialize the strategy with the given parameters.

        Args:
            symbol: Trading symbol identifier
            params: Strategy configuration parameters
        """
        self.symbol = symbol
        self.params = params
        self.buffer_size = max(params.ma_long, params.rsi_period + 1)
        self.prices: Deque[float] = deque(maxlen=self.buffer_size)
        self.last_signal = SignalType.HOLD

        # Initialize RSI indicator if enabled
        self.rsi_indicator = RSIIndicator(period=params.rsi_period) if params.use_rsi else None

        # Initialize moving averages
        self.last_ma_short: Optional[float] = None
        self.last_ma_long: Optional[float] = None

        # Initialize logger
        from common.logging_helper import get_logger
        self.logger = get_logger(f"{self.__class__.__name__}_{symbol}")

    def _calculate_moving_averages(self) -> tuple[float, float]:
        """Calculate both moving averages efficiently."""
        prices_list = list(self.prices)
        ma_short = sum(prices_list[-self.params.ma_short:]) / self.params.ma_short
        ma_long = sum(prices_list[-self.params.ma_long:]) / self.params.ma_long
        return ma_short, ma_long

    def _check_rsi_signal(self, price: float, timestamp: str) -> Optional[Dict[str, Any]]:
        """Generate RSI-based trading signal if conditions are met."""
        if not self.rsi_indicator or len(self.prices) < self.params.rsi_period + 1:
            return None

        try:
            rsi_value = self.rsi_indicator.compute(list(self.prices))

            if rsi_value <= self.params.rsi_buy_threshold:
                return self._create_signal(SignalType.BUY, SignalSource.RSI, price, timestamp, rsi=rsi_value)
            elif rsi_value >= self.params.rsi_sell_threshold:
                return self._create_signal(SignalType.SELL, SignalSource.RSI, price, timestamp, rsi=rsi_value)

        except Exception as e:
            self.logger.error(
                "Error calculating RSI signal",
                extra={
                    "error": str(e),
                    "symbol": self.symbol,
                    "prices_length": len(self.prices)
                }
            )
        return None

    def _check_ma_signal(self, price: float, timestamp: str) -> Optional[Dict[str, Any]]:
        """Generate Moving Average-based trading signal if conditions are met."""
        if len(self.prices) < max(self.params.ma_short, self.params.ma_long):
            return None

        try:
            ma_short, ma_long = self._calculate_moving_averages()

            if ma_short > ma_long:
                return self._create_signal(SignalType.BUY, SignalSource.MA, price, timestamp)
            elif ma_short < ma_long:
                return self._create_signal(SignalType.SELL, SignalSource.MA, price, timestamp)

        except Exception as e:
            self.logger.error(
                "Error calculating MA signal",
                extra={
                    "error": str(e),
                    "symbol": self.symbol,
                    "prices_length": len(self.prices)
                }
            )
        return None

    def _create_signal(
            self,
            signal_type: SignalType,
            source: SignalSource,
            price: float,
            timestamp: str,
            **extra_data
    ) -> Dict[str, Any]:
        """Create a standardized signal dictionary."""
        signal = {
            "signal_type": signal_type.value,
            "symbol": self.symbol,
            "timestamp": timestamp,
            "price": price,
            "source": source.value
        }
        signal.update(extra_data)
        return signal

    def update(self, timestamp: str, price: float) -> Optional[Dict[str, Any]]:
        """
        Update the strategy with new price data and generate trading signals.

        Args:
            timestamp: Current timestamp
            price: Current price

        Returns:
            Optional trading signal dictionary if a signal is generated
        """
        start_time = time.time()
        signal = None

        try:
            self.prices.append(price)

            # Check RSI signal first if enabled
            if self.params.use_rsi:
                signal = self._check_rsi_signal(price, timestamp)

            # Check MA signal if no RSI signal was generated
            if signal is None:
                signal = self._check_ma_signal(price, timestamp)

            return signal

        except Exception as e:
            self.logger.error(
                "Error in strategy update",
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
                "SymbolStrategy update completed",
                extra={
                    "computation_time": comp_time,
                    "symbol": self.symbol,
                    "prices_length": len(self.prices),
                    "signal": signal
                }
            )