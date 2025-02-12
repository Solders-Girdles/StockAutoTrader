import unittest
from quant.strategies.bollinger_bands_strategy import BollingerBandsStrategy

class TestBollingerBandsStrategy(unittest.TestCase):
    def setUp(self):
        self.strategy = BollingerBandsStrategy(symbol="TEST")
        self.timestamp = "2025-02-10T12:00:00Z"

    def test_initialization(self):
        self.assertEqual(self.strategy.symbol, "TEST")
        self.assertEqual(self.strategy.window, 20)
        self.assertEqual(self.strategy.multiplier, 2.0)
        self.assertEqual(self.strategy.prices, [])

    def test_update_no_signal(self):
        # Not enough data for a signal
        signal = self.strategy.update(self.timestamp, 100)
        self.assertEqual(signal, {})

    def test_update_buy_signal(self):
        # Simulate data for a BUY signal (price below lower band)
        prices = [100] * 19 + [90]  # 19 values at 100, then one at 90
        signal = {}
        for price in prices:
            temp_signal = self.strategy.update(self.timestamp, price)
            if temp_signal:
                signal = temp_signal
        self.assertNotEqual(signal, {})
        self.assertEqual(signal["signal_type"], "BUY")
        self.assertEqual(signal["action"], "BUY")
        self.assertEqual(signal["source"], "Bollinger")
        self.assertIn("confidence", signal)
        self.assertIn("sma", signal)
        self.assertIn("upper_band", signal)
        self.assertIn("lower_band", signal)

    def test_update_sell_signal(self):
        # Simulate data for a SELL signal (price above upper band)
        prices = [100] * 19 + [110]  # 19 values at 100, then one at 110
        signal = {}
        for price in prices:
            temp_signal = self.strategy.update(self.timestamp, price)
            if temp_signal:
                signal = temp_signal

        self.assertNotEqual(signal, {})
        self.assertEqual(signal["signal_type"], "SELL")
        self.assertEqual(signal["action"], "SELL")
        self.assertEqual(signal["source"], "Bollinger")
        self.assertIn("confidence", signal)
        self.assertIn("sma", signal)
        self.assertIn("upper_band", signal)
        self.assertIn("lower_band", signal)


if __name__ == '__main__':
    unittest.main()