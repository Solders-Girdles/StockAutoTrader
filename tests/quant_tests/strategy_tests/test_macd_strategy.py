import unittest
from quant.strategies.macd_strategy import MACDStrategy
import pandas as pd

class TestMACDStrategy(unittest.TestCase):
    def setUp(self):
        # Initialize the strategy with default MACD parameters.
        self.strategy = MACDStrategy(symbol="TEST", short_period=12, long_period=26, signal_period=9)
        self.timestamp = "2025-02-10T12:00:00Z"

    def test_initialization(self):
        # Check that the strategy is initialized correctly.
        self.assertEqual(self.strategy.symbol, "TEST")
        self.assertEqual(self.strategy.short_period, 12)
        self.assertEqual(self.strategy.long_period, 26)
        self.assertEqual(self.strategy.signal_period, 9)
        self.assertEqual(self.strategy.prices, [])
        self.assertEqual(self.strategy.macd_history, [])

    def test_update_no_signal(self):
        # Test that with insufficient data, update returns an empty dict.
        signal = self.strategy.update(self.timestamp, 100)
        self.assertEqual(signal, {}, "Expected an empty dict when insufficient data is provided.")

    def test_update_buy_signal(self):
        # Simulate a series of increasing prices that should eventually trigger a BUY signal.
        signal = {}
        prices = [100 + i for i in range(40)]  # 40 prices provide enough data
        for price in prices:
            temp_signal = self.strategy.update(self.timestamp, price)
            if temp_signal:
                signal = temp_signal
        self.assertTrue(signal, "Expected a signal for increasing prices.")
        self.assertEqual(signal.get("signal_type"), "BUY", f"Expected BUY signal, got: {signal}")
        self.assertEqual(signal.get("action"), "BUY", "Action should be BUY.")
        self.assertEqual(signal.get("source"), "MACD", "Source should be MACD.")
        self.assertIn("confidence", signal, "Signal should include a confidence value.")
        self.assertIn("macd", signal, "Signal should include MACD value.")
        self.assertIn("signal_line", signal, "Signal should include signal_line value.")
        self.assertIn("histogram", signal, "Signal should include histogram value.")

    def test_update_sell_signal(self):
        # Reset the strategy for a new SELL test.
        self.strategy = MACDStrategy(symbol="TEST", short_period=12, long_period=26, signal_period=9)
        signal = {}
        prices = [130 - i for i in range(40)]  # Decreasing prices
        for price in prices:
            temp_signal = self.strategy.update(self.timestamp, price)
            if temp_signal:
                signal = temp_signal
        self.assertTrue(signal, "Expected a signal for decreasing prices.")
        self.assertEqual(signal.get("signal_type"), "SELL", f"Expected SELL signal, got: {signal}")
        self.assertEqual(signal.get("action"), "SELL", "Action should be SELL.")
        self.assertEqual(signal.get("source"), "MACD", "Source should be MACD.")
        self.assertIn("confidence", signal, "Signal should include a confidence value.")
        self.assertIn("macd", signal, "Signal should include MACD value.")
        self.assertIn("signal_line", signal, "Signal should include signal_line value.")
        self.assertIn("histogram", signal, "Signal should include histogram value.")

    def test_no_duplicate_signal(self):
        """
        Verify that once a signal is triggered, subsequent updates with similar data do not produce a new signal.
        """
        signal = {}
        prices = [100, 105, 110, 115, 120, 125, 130, 135, 140]
        for price in prices:
            temp_signal = self.strategy.update(self.timestamp, price)
            if temp_signal:
                signal = temp_signal
        first_signal = signal.copy()
        # Continue updating with the same price sequence
        for price in prices:
            signal = self.strategy.update(self.timestamp, price)
        # Expect no new signal (i.e. an empty dict) because the same signal is already active.
        self.assertEqual(signal, {}, "Expected no duplicate signal once a signal is already active.")

    def _calculate_pandas_ema(self, data, period, prev_ema=None):
        data_series = pd.Series(data)
        if prev_ema is not None:
            # Simulate EMA calculation with a previous EMA value.
            alpha = 2 / (period + 1)
            return (data_series.iloc[-1] - prev_ema) * alpha + prev_ema
        # Otherwise, compute the EMA using pandas' ewm.
        return data_series.ewm(span=period, adjust=False).mean().iloc[-1]

    def test_ema_calculation(self):
        """
        Test the internal _ema method by comparing its output with pandas' EMA calculation.
        """
        data = [10, 12, 15, 14, 16]
        period = 3
        ema = self.strategy._ema(data, period)
        pandas_ema = self._calculate_pandas_ema(data, period)
        self.assertAlmostEqual(ema, pandas_ema, places=2, msg="EMA calculation should match pandas' result.")

        # Test with a previous EMA value.
        prev_ema = 14.0
        ema = self.strategy._ema(data, period, prev_ema)
        pandas_ema = self._calculate_pandas_ema(data, period, prev_ema)
        self.assertAlmostEqual(ema, pandas_ema, places=2, msg="EMA calculation with prev_ema should match pandas' result.")

if __name__ == '__main__':
    unittest.main()