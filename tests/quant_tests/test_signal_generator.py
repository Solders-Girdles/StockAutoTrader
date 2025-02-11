import os
import sys

# Compute the project root: two levels up from the tests/quant_tests directory.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
import unittest
import quant.strategies as strategies
# or, if you need a specific function or class:
from quant.strategies import SymbolStrategy


class TestSignalGeneration(unittest.TestCase):
    def test_signal_generation_valid_data(self):
        # Provide sample market data.
        market_data = {
            "symbol": "AAPL",
            "open": 150,
            "high": 155,
            "low": 149,
            "close": 154,
            "volume": 1000000,
            "vwap": 152,
            "timestamp": "2025-02-10T17:00:00Z"
        }
        signal = signal_generator.generate_signal(market_data)
        # Assert that the generated signal is within expected parameters.
        self.assertIn(signal, ["buy", "sell", "hold"])


if __name__ == "__main__":
    unittest.main()