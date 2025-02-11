import os
import sys
import unittest

# Ensure the project root is added to sys.path so that the production package is found.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the SymbolStrategy class from quant/strategies.py
from quant.strategies import SymbolStrategy


class TestSignalGeneration(unittest.TestCase):
    def test_signal_generation_valid_data(self):
        """
        Test that given valid market data, the strategy produces a valid signal.
        """
        # Define sample market data that matches the expected format.
        market_data = {
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

        # Instantiate a SymbolStrategy.
        # Adjust parameters as required by your implementation.
        strategy = SymbolStrategy(
            symbol="TEST",
            ma_short=2,
            ma_long=3,
            rsi_period=14,
            use_rsi=False,
            rsi_buy=30,
            rsi_sell=70
        )

        # Feed the strategy with data to build up its internal state.
        # It may require several updates to produce a signal.
        signal = None
        for _ in range(5):
            signal = strategy.update(market_data["timestamp"], market_data["close"])

        # Assert that a signal was generated (i.e., signal is not None).
        self.assertIsNotNone(signal, "Expected a signal to be generated from valid market data")


if __name__ == "__main__":
    unittest.main()