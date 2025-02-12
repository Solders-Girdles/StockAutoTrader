from typing import List, Tuple, Optional


class StochasticOscillator:
    """
    Stochastic Oscillator Indicator.

    The Stochastic Oscillator is a momentum indicator that shows the location of the closing price
    relative to the high-low range over a set number of periods.
    """

    def __init__(self, k_period: int = 14, d_period: int = 3) -> None:
        """
        Initialize the Stochastic Oscillator with specified periods.

        Args:
            k_period: The lookback period for %K calculation (default: 14)
            d_period: The smoothing period for %D calculation (default: 3)

        Raises:
            ValueError: If k_period or d_period is not a positive integer
        """
        if not isinstance(k_period, int) or k_period <= 0:
            raise ValueError("k_period must be a positive integer")
        if not isinstance(d_period, int) or d_period <= 0:
            raise ValueError("d_period must be a positive integer")

        self.k_period = k_period
        self.d_period = d_period
        self.k_values: List[float] = []

    def compute(self, prices: List[float]) -> Tuple[Optional[float], Optional[float]]:
        """
        Calculate the Stochastic Oscillator values for the given price series.

        Args:
            prices: List of price values

        Returns:
            Tuple of (K value, D value). Either value may be None if insufficient data.
        """
        if not prices:
            return None, None

        if len(prices) < self.k_period:
            return None, None

        # Calculate %K
        high_k = max(prices[-self.k_period:])
        low_k = min(prices[-self.k_period:])

        if high_k == low_k:
            k = 50.0
        else:
            k = ((prices[-1] - low_k) / (high_k - low_k)) * 100

        self.k_values.append(k)

        # Calculate %D (SMA of %K)
        if len(self.k_values) >= self.d_period:
            d = sum(self.k_values[-self.d_period:]) / self.d_period
            return k, d

        return k, None