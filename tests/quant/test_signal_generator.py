import unittest
from quant import .signal_generator


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