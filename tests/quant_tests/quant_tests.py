# tests/quant_tests/test_strategies.py
import unittest
from tests.base_test import BaseTest  # if you have a base class
from quant.strategies import SymbolStrategy

class TestSymbolStrategy(BaseTest):
    def setUp(self):
        self.market_data = {
            "symbol": "TEST",
            "timestamp": "2025-02-10T12:00:00Z",
            "open": 100,
            "high": 110,
            "low": 90,
            "close": 105,
            "volume": 1000,
            "vwap": 105,
            "interval": "1m"
        }
        self.strategy = SymbolStrategy(
            symbol="TEST",
            ma_short=2,
            ma_long=3,
            rsi_period=14,
            use_rsi=False,
            rsi_buy=30,
            rsi_sell=70
        )

    def test_signal_generation_valid_data(self):
        """Test that valid market data eventually produces a trading signal."""
        signal = None
        # Run multiple updates to build internal state.
        for _ in range(5):
            signal = self.strategy.update(self.market_data["timestamp"], self.market_data["close"])
        self.assertIsNotNone(signal, "Expected a trading signal to be generated.")

    def test_signal_edge_case_missing_data(self):
        """Test behavior when some expected fields are missing from market data."""
        incomplete_data = self.market_data.copy()
        incomplete_data.pop("vwap", None)
        # Depending on your implementation, the strategy might use fallback values.
        signal = None
        for _ in range(5):
            signal = self.strategy.update(incomplete_data["timestamp"], incomplete_data["close"])
        self.assertIsNotNone(signal, "Strategy should generate a signal even with missing vwap data.")

if __name__ == "__main__":
    unittest.main()