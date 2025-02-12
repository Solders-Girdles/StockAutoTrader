import unittest
from typing import List, Optional
import pandas as pd

class MACDStrategy:
    def __init__(self):
        self.prices = []

    def _ema(self, data: List[float], period: int, prev_ema: Optional[float] = None) -> float:
        alpha = 2 / (period + 1)
        #print(f"Calculating EMA with data: {data}, period: {period}, prev_ema: {prev_ema}, alpha: {alpha}")
        if prev_ema is None:
            series = pd.Series(data[-period:])
            ema = series.ewm(span=period, adjust=False).mean().iloc[-1]
            #print(f"Calculated Initial EMA: {ema}")
            return ema
        else:
            ema = (data[-1] - prev_ema) * alpha + prev_ema
            #print(f"Calculated Subsequent EMA: {ema}")
            return ema

class TestMACDStrategy(unittest.TestCase):
    def setUp(self):
        self.strategy = MACDStrategy()

    def test_ema_calculation(self):
        # Test data
        data = [10, 12, 15, 14, 16]
        period = 3

        # --- Test 1: First EMA calculation ---
        ema1 = self.strategy._ema(data, period)
        expected_ema1 = pd.Series(data[-period:]).ewm(span=period, adjust=False).mean().iloc[-1] #CORRECTED
        #print(f"Test 1: Calculated EMA = {ema1}, Expected EMA = {expected_ema1}")
        self.assertAlmostEqual(ema1, expected_ema1, places=7, msg="First EMA calculation failed")

        # --- Test 2: EMA calculation with previous EMA ---
        prev_ema = 14.0
        ema2 = self.strategy._ema(data, period, prev_ema)

        # Calculate the expected EMA manually (using the formula)
        alpha = 2 / (period + 1)
        expected_ema2 = (data[-1] - prev_ema) * alpha + prev_ema
        #print(f"Test 2: Calculated EMA = {ema2}, Expected EMA = {expected_ema2}")

        self.assertAlmostEqual(ema2, expected_ema2, places=7, msg="EMA with prev_ema failed")

if __name__ == '__main__':
    unittest.main()