import unittest
from quant.strategies.symbol_strategy import SymbolStrategy, StrategyParameters, SignalType, SignalSource


class TestSymbolStrategy(unittest.TestCase):
    def setUp(self):
        """Initialize strategy instances for testing."""
        # Base parameters without RSI
        self.base_params = StrategyParameters(
            ma_short=2,
            ma_long=3,
            rsi_period=14,
            use_rsi=False,
            rsi_buy_threshold=30.0,
            rsi_sell_threshold=70.0
        )

        # Create strategy instance
        self.strategy = SymbolStrategy(
            symbol="TEST",
            params=self.base_params
        )

        self.timestamp = "2025-02-10T12:00:00Z"

    def test_initialization(self):
        """Test proper strategy initialization."""
        self.assertEqual(self.strategy.symbol, "TEST")
        self.assertEqual(self.strategy.buffer_size, max(
            self.base_params.ma_long,
            self.base_params.rsi_period + 1
        ))
        self.assertEqual(self.strategy.last_signal, SignalType.HOLD)

    def test_signal_generation_with_constant_data(self):
        """Test that constant prices generate no signals."""
        signal = None
        for price in [100] * 5:
            signal = self.strategy.update(self.timestamp, price)
        self.assertIsNone(signal, "No signal should be generated when price is constant.")

    def test_signal_generation_with_increasing_data(self):
        """Test buy signal generation with increasing prices."""
        prices = [100, 105, 110, 115, 120, 125, 130]
        signal = None
        for price in prices:
            temp_signal = self.strategy.update(self.timestamp, price)
            if temp_signal:
                signal = temp_signal

        self.assertIsNotNone(signal, "Expected a signal for increasing prices.")
        self.assertEqual(signal["signal_type"], SignalType.BUY.value)
        self.assertEqual(signal["symbol"], "TEST")
        self.assertEqual(signal["timestamp"], self.timestamp)
        self.assertEqual(signal["source"], SignalSource.MA.value)
        self.assertTrue(isinstance(signal["price"], (int, float)))

    def test_signal_generation_with_decreasing_data(self):
        """Test sell signal generation with decreasing prices."""
        prices = [130, 125, 120, 115, 110, 105, 100]
        signal = None
        for price in prices:
            temp_signal = self.strategy.update(self.timestamp, price)
            if temp_signal:
                signal = temp_signal

        self.assertIsNotNone(signal)
        self.assertEqual(signal["signal_type"], SignalType.SELL.value)
        self.assertEqual(signal["source"], SignalSource.MA.value)

    def test_no_signal_before_ma_long(self):
        """Test that no signals are generated before sufficient data is available."""
        prices = [100, 105]  # Not enough for ma_long = 3
        for price in prices:
            signal = self.strategy.update(self.timestamp, price)
            self.assertIsNone(signal)

    def test_rsi_signals(self):
        """Test RSI signal generation with both buy and sell scenarios."""
        # Initialize strategy with RSI enabled
        rsi_params = StrategyParameters(
            ma_short=2,
            ma_long=3,
            rsi_period=5,
            use_rsi=True,
            rsi_buy_threshold=30.0,
            rsi_sell_threshold=70.0
        )
        strategy_rsi = SymbolStrategy(symbol="TEST_RSI", params=rsi_params)

        # Test RSI buy signal (oversold condition)
        decreasing_prices = [120, 115, 110, 105, 100, 95]
        signal = None
        for price in decreasing_prices:
            temp_signal = strategy_rsi.update(self.timestamp, price)
            if temp_signal:
                signal = temp_signal

        self.assertIsNotNone(signal)
        self.assertEqual(signal["signal_type"], SignalType.BUY.value)
        self.assertTrue(signal["rsi"] <= rsi_params.rsi_buy_threshold)
        self.assertEqual(signal["source"], SignalSource.RSI.value)

        # Test RSI sell signal (overbought condition)
        strategy_rsi = SymbolStrategy(symbol="TEST_RSI", params=rsi_params)
        increasing_prices = [95, 100, 105, 110, 115, 120]
        signal = None
        for price in increasing_prices:
            temp_signal = strategy_rsi.update(self.timestamp, price)
            if temp_signal:
                signal = temp_signal

        self.assertIsNotNone(signal)
        self.assertEqual(signal["signal_type"], SignalType.SELL.value)
        self.assertTrue(signal["rsi"] >= rsi_params.rsi_sell_threshold)
        self.assertEqual(signal["source"], SignalSource.RSI.value)

    def test_no_rsi_signal_before_period(self):
        """Test that no RSI signals are generated before sufficient data is available."""
        rsi_params = StrategyParameters(
            ma_short=2,
            ma_long=3,
            rsi_period=5,
            use_rsi=True,
            rsi_buy_threshold=30.0,
            rsi_sell_threshold=70.0
        )
        strategy_rsi = SymbolStrategy(symbol="TEST_RSI", params=rsi_params)

        prices = [100, 105]  # Insufficient data for RSI calculation
        for price in prices:
            signal = strategy_rsi.update(self.timestamp, price)
            self.assertIsNone(signal)

    def test_signal_priority(self):
        """Test that RSI signals take priority over MA signals when both are available."""
        rsi_params = StrategyParameters(
            ma_short=2,
            ma_long=3,
            rsi_period=5,
            use_rsi=True,
            rsi_buy_threshold=30.0,
            rsi_sell_threshold=70.0
        )
        strategy_rsi = SymbolStrategy(symbol="TEST_RSI", params=rsi_params)

        # Create conditions where both RSI and MA would generate signals
        prices = [120, 115, 110, 105, 100, 95]  # Should trigger RSI buy
        signal = None
        for price in prices:
            temp_signal = strategy_rsi.update(self.timestamp, price)
            if temp_signal:
                signal = temp_signal

        self.assertEqual(signal["source"], SignalSource.RSI.value)


if __name__ == "__main__":
    unittest.main()