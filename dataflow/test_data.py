import random
import time
import math
import datetime


def generate_test_data(symbols, cycle_interval=1.0):
    """
    A generator function that yields synthetic market data for given symbols.

    :param symbols: A list of symbols to generate data for, e.g. ['AAPL', 'MSFT', 'GOOG'].
    :param cycle_interval: How many seconds to wait between each batch of updates.
    :yields: A dictionary representing the mock market data for one symbol.
    """
    # We can cycle a 'wave' or use randomwalk for price
    # For simplicity, we'll do a random price with a slight incremental wave.

    wave_phase = 0.0

    while True:
        for sym in symbols:
            # Example random base price between 100-200
            base_price = 150.0
            # Add a small wave effect + random jitter
            wave = math.sin(wave_phase) * 5.0 + random.uniform(-1.0, 1.0)

            price = base_price + wave
            volume = random.randint(500, 2000)

            # Generate a synthetic timestamp
            timestamp = datetime.datetime.utcnow().isoformat()

            # Prepare the mock market data
            data = {
                "symbol": sym,
                "price": round(price, 2),
                "volume": volume,
                "timestamp": timestamp
            }

            yield data

        # Increment the wave phase
        wave_phase += 0.3

        # Sleep before next cycle of data
        time.sleep(cycle_interval)