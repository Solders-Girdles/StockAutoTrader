import os
import sys
import unittest

# Ensure the project root is in sys.path.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from quant.indicators.atr_indicator import ATRIndicator

class TestATRIndicator(unittest.TestCase):
    def setUp(self):
        # Create an ATRIndicator with a 14-period.
        self.atr = ATRIndicator(period=14)

    def test_insufficient_data(self):
        """
        Test that compute returns None when there is insufficient data.
        We'll simulate this by incrementally providing a short series.
        """
        prices = [100, 101, 102]  # Only 3 data points.
        highs = [100, 102, 103]
        lows = [99, 100, 101]
        result = None
        for i in range(2, len(prices)+1):
            result = self.atr.compute(prices[:i], highs[:i], lows[:i])
        self.assertIsNone(result, "ATR should be None when insufficient data is provided.")

    def test_inconsistent_lengths(self):
        """
        Test that compute raises a ValueError when input lists are not of equal length.
        """
        prices = [100, 101, 102, 103]
        highs = [101, 102, 103, 104]
        lows = [99, 100, 101]  # One value fewer than prices and highs.
        with self.assertRaises(ValueError):
            self.atr.compute(prices, highs, lows)

    def test_constant_prices_returns_zero(self):
        """
        With constant prices, highs, and lows, the true range is 0,
        so after enough data points, ATR should be 0.
        We simulate a rolling update through the entire series.
        """
        prices = [100] * 20
        highs = [100] * 20
        lows = [100] * 20
        result = None
        for i in range(2, len(prices) + 1):
            result = self.atr.compute(prices[:i], highs[:i], lows[:i])
        self.assertEqual(result, 0.0, "ATR should be 0 when price, high, and low are constant.")

    def test_normal_data(self):
        """
        Test ATR calculation with a realistic scenario.
        We simulate rolling updates; when there are enough data points,
        compute() should return a float representing the ATR.
        """
        prices = [100, 102, 101, 105, 107, 106, 108, 110, 109, 112, 115, 113, 116, 117, 119, 120]
        highs  = [101, 103, 102, 106, 108, 107, 109, 111, 110, 113, 116, 114, 117, 118, 120, 121]
        lows   = [99, 100, 100, 104, 106, 105, 107, 109, 108, 111, 113, 112, 115, 116, 118, 119]
        result = None
        for i in range(2, len(prices) + 1):
            result = self.atr.compute(prices[:i], highs[:i], lows[:i])
        self.assertIsInstance(result, float, "ATR should return a float when enough data is provided.")
        self.assertGreaterEqual(result, 0.0, "ATR should be non-negative.")

if __name__ == "__main__":
    unittest.main()