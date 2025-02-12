import os
import sys
import unittest

# Ensure the project root is in sys.path.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from quant.indicators.rsi_indicator import RSIIndicator

class TestRSIIndicator(unittest.TestCase):
    def setUp(self):
        # Create an RSIIndicator with a 14-period.
        self.rsi = RSIIndicator(period=14)

    def test_insufficient_data_returns_neutral(self):
        # With less than 15 price points, expect a neutral RSI (50.0)
        prices = [100] * 10  # only 10 points
        result = self.rsi.compute(prices)
        self.assertEqual(result, 50.0, "RSI should be neutral (50.0) when data is insufficient.")

    def test_constant_prices_returns_neutral(self):
        # With enough price points but all constant, RSI should be neutral.
        prices = [100] * 16  # 16 points ensures we have period+1 data
        result = self.rsi.compute(prices)
        self.assertEqual(result, 50.0, "RSI should be neutral when prices are constant.")

    def test_all_gains_returns_100(self):
        # Generate prices that strictly increase.
        prices = [100 + i for i in range(16)]  # 16 prices provides 15 differences.
        result = self.rsi.compute(prices)
        self.assertEqual(result, 100.0, "RSI should be 100 when all changes are gains.")

    def test_all_losses_returns_0(self):
        # Generate prices that strictly decrease.
        prices = [100 - i for i in range(16)]
        result = self.rsi.compute(prices)
        self.assertEqual(result, 0.0, "RSI should be 0 when all changes are losses.")

    def test_mixed_changes(self):
        # A sequence with both gains and losses; expected RSI value computed manually.
        # Replace this sequence with your own if necessary.
        prices = [44, 44.15, 43.61, 43.77, 43.33, 42.90, 43.10, 43.15, 43.61, 43.78, 43.78, 43.37, 43.42, 43.25, 43.45, 43.56]
        result = self.rsi.compute(prices)
        # For demonstration, we assume the expected RSI is approximately 43.
        # Adjust the expected value and tolerance (delta) as needed.
        self.assertAlmostEqual(result, 43, delta=2, msg="RSI should be near the expected value for mixed changes.")

if __name__ == "__main__":
    unittest.main()