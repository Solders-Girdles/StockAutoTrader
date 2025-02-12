import unittest
from typing import List, Optional, Tuple
from quant.indicators.stochastic_oscillator import StochasticOscillator


class TestStochasticOscillator(unittest.TestCase):
    """
    Test suite for the Stochastic Oscillator technical indicator.
    """

    def setUp(self) -> None:
        """Initialize oscillator instances and test data."""
        self.default_oscillator = StochasticOscillator()
        self.custom_oscillator = StochasticOscillator(k_period=5, d_period=2)

        # Common test data
        self.basic_prices = [10, 12, 15, 14, 16, 18, 20, 19, 17, 15, 14, 12, 10, 11, 13]
        self.trending_prices = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
        self.volatile_prices = [10, 20, 10, 20, 10, 20, 10, 20, 10, 20]

    def _calculate_expected_values(
            self,
            prices: List[float],
            oscillator: StochasticOscillator
    ) -> Tuple[List[Optional[float]], List[Optional[float]]]:
        """Calculate expected K and D values for verification."""
        expected_k_values: List[Optional[float]] = []
        expected_d_values: List[Optional[float]] = []
        k_values: List[float] = []

        for i in range(len(prices)):
            if i + 1 < oscillator.k_period:
                expected_k_values.append(None)
                expected_d_values.append(None)
                continue

            window = prices[i - oscillator.k_period + 1:i + 1]
            high_k = max(window)
            low_k = min(window)

            if high_k == low_k:
                k = 50.0
            else:
                k = ((prices[i] - low_k) / (high_k - low_k)) * 100

            k_values.append(k)
            expected_k_values.append(k)

            if len(k_values) >= oscillator.d_period:
                d = sum(k_values[-oscillator.d_period:]) / oscillator.d_period
                expected_d_values.append(d)
            else:
                expected_d_values.append(None)

        return expected_k_values, expected_d_values

    def _verify_oscillator_values(
            self,
            prices: List[float],
            expected_k_values: List[Optional[float]],
            expected_d_values: List[Optional[float]],
            oscillator: StochasticOscillator
    ) -> None:
        """Verify oscillator calculations against expected values."""
        # Create new oscillator instance to ensure clean state
        fresh_oscillator = StochasticOscillator(
            k_period=oscillator.k_period,
            d_period=oscillator.d_period
        )

        for i in range(len(prices)):
            k, d = fresh_oscillator.compute(prices[:i + 1])

            if expected_k_values[i] is None:
                self.assertIsNone(k, f"K should be None at step {i + 1}")
            else:
                self.assertIsNotNone(k, f"K should not be None at step {i + 1}")
                self.assertAlmostEqual(
                    k,
                    expected_k_values[i],
                    places=2,
                    msg=f"K value mismatch at step {i + 1}"
                )

            if expected_d_values[i] is None:
                self.assertIsNone(d, f"D should be None at step {i + 1}")
            else:
                self.assertIsNotNone(d, f"D should not be None at step {i + 1}")
                self.assertAlmostEqual(
                    d,
                    expected_d_values[i],
                    places=2,
                    msg=f"D value mismatch at step {i + 1}"
                )

    def test_input_validation(self) -> None:
        """Test input validation for constructor parameters."""
        with self.assertRaises(ValueError):
            StochasticOscillator(k_period=0)

        with self.assertRaises(ValueError):
            StochasticOscillator(d_period=0)

        with self.assertRaises(ValueError):
            StochasticOscillator(k_period=-1)

        with self.assertRaises(ValueError):
            StochasticOscillator(k_period="14")

    def test_empty_data(self) -> None:
        """Test behavior with empty price data."""
        k, d = self.default_oscillator.compute([])
        self.assertIsNone(k)
        self.assertIsNone(d)

    def test_insufficient_data(self) -> None:
        """Test behavior with insufficient data for calculations."""
        oscillator = StochasticOscillator(k_period=5, d_period=3)
        prices = [10, 11, 12, 13, 14]

        # Test with exactly k_period points
        k, d = oscillator.compute(prices)
        self.assertIsNotNone(k)
        self.assertIsNone(d)

    def test_memory_consistency(self) -> None:
        """Test state management between computations."""
        oscillator = StochasticOscillator(k_period=3, d_period=2)
        prices = [10, 15, 12, 18]

        # Compute values for increasing subsets of data
        k1, d1 = oscillator.compute(prices[:3])
        k2, d2 = oscillator.compute(prices[:4])

        self.assertIsNotNone(k1)
        self.assertIsNotNone(k2)
        self.assertNotEqual(k1, k2)


if __name__ == '__main__':
    unittest.main()