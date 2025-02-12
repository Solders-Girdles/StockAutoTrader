import time
import numpy as np
from typing import Dict, Any, List
from common.logging_helper import get_logger

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
        self.logger = get_logger(self.__class__.__name__)

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
        self.logger.debug("BollingerBandsStrategy update", extra={"computation_time": comp_time})  # Use self.logger
        return result