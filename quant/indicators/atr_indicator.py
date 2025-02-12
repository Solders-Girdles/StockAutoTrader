from typing import List, Optional
from collections import deque


class ATRIndicator:
    """
    Average True Range (ATR) Indicator.

    The ATR measures market volatility by averaging the True Range over a specified period.
    The True Range for a period is defined as the maximum of:
      - Current high - current low
      - Absolute value of (current high - previous close)
      - Absolute value of (current low - previous close)

    This implementation maintains a stateful deque of the last 'period' True Range values.
    If there is insufficient data, the compute() method returns None.
    """

    def __init__(self, period: int = 14) -> None:
        """
        Initialize the ATRIndicator.

        Args:
            period (int): The number of periods to average over for ATR calculation.
        """
        self.period = period
        # Use a deque to automatically cap the stored true range values at the defined period.
        self.true_ranges = deque(maxlen=period)

    def compute(self, prices: List[float], highs: List[float], lows: List[float]) -> Optional[float]:
        """
        Compute the ATR based on the latest price data.

        The function expects three lists of equal length:
          - prices: Closing prices.
          - highs: High prices.
          - lows: Low prices.

        These lists must be ordered chronologically. The ATR is calculated using the most
        recent values (the current period) as follows:
          - current_high = highs[-1]
          - current_low = lows[-1]
          - previous_close = prices[-2]
          - true_range = max(
                current_high - current_low,
                abs(current_high - previous_close),
                abs(current_low - previous_close)
            )
          - The true_range is appended to the deque, and if at least 'period' values are present,
            ATR is returned as the simple average of these values.

        Returns:
            Optional[float]: The ATR value, or None if there is insufficient data.

        Raises:
            ValueError: If the input lists are not of equal length.
        """
        # Validate that there are at least two data points.
        if len(prices) < 2 or len(highs) < 2 or len(lows) < 2:
            return None

        # Ensure the input lists are of the same length.
        if not (len(prices) == len(highs) == len(lows)):
            raise ValueError("The lengths of prices, highs, and lows must be equal.")

        # Compute true range using the latest available data.
        current_high = highs[-1]
        current_low = lows[-1]
        previous_close = prices[-2]

        true_range = max(
            current_high - current_low,
            abs(current_high - previous_close),
            abs(current_low - previous_close)
        )
        self.true_ranges.append(true_range)

        # Return None if insufficient true range values have been collected.
        if len(self.true_ranges) < self.period:
            return None

        # Calculate ATR as the simple average of the stored true range values.
        atr = sum(self.true_ranges) / self.period
        return atr