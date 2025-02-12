from typing import List
from collections import deque
from datetime import datetime, timezone

class RSIIndicator:
    """
    Relative Strength Index (RSI) Indicator

    Computes the RSI over a fixed rolling window of price data.
    The RSI is a momentum oscillator that measures the speed and change
    of price movements, yielding a value between 0 and 100:
      - 50 indicates a neutral state.
      - Values near 100 indicate overbought conditions.
      - Values near 0 indicate oversold conditions.

    If there is insufficient data (fewer than period+1 price points),
    this indicator returns a neutral RSI value of 50.

    The computation is stateless; each call to compute() uses only the provided
    price list, ensuring no side effects from previous calculations.
    """

    def __init__(self, period: int = 14) -> None:
        """
        Initialize the RSIIndicator with a specified period.

        Args:
            period (int): Number of periods over which RSI is calculated.
                          Defaults to 14.
        """
        self.period = period

    def compute(self, prices: List[float]) -> float:
        """
        Compute the RSI for the provided list of prices.

        Args:
            prices (List[float]): List of price values. Must contain at least period+1 values.

        Returns:
            float: The computed RSI value rounded to two decimal places.
                   Returns 50.0 if insufficient data is provided.
        """
        # Check if there are enough price points to calculate RSI.
        if len(prices) < self.period + 1:
            return 50.0  # Neutral value

        # Consider only the most recent 'period+1' prices.
        window = prices[-(self.period + 1):]

        # Use local deques for gains and losses to keep the computation stateless.
        gains = deque(maxlen=self.period)
        losses = deque(maxlen=self.period)

        # Compute the gain or loss for each consecutive pair in the window.
        for i in range(1, len(window)):
            change = window[i] - window[i - 1]
            if change > 0:
                gains.append(change)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(abs(change))

        # Calculate the average gain and loss.
        avg_gain = sum(gains) / self.period
        avg_loss = sum(losses) / self.period

        # Handle edge cases:
        # - If both average gain and loss are zero, prices are constant; return neutral.
        if avg_gain == 0 and avg_loss == 0:
            return 50.0
        # - If there are no losses, RSI is defined as 100.
        if avg_loss == 0:
            return 100.0

        # Compute the Relative Strength (RS) and then the RSI.
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return round(rsi, 2)