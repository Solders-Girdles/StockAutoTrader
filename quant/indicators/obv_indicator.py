class OBVIndicator:
    """
    On-Balance Volume (OBV) Indicator.
    """
    def __init__(self) -> None:
        self.obv = 0
        self.prev_close = None

    def compute(self, close: float, volume: float) -> int:
        if self.prev_close is None:
            self.prev_close = close
            return self.obv  # Return initial OBV (0)

        if close > self.prev_close:
            self.obv += volume
        elif close < self.prev_close:
            self.obv -= volume
        # else: OBV remains unchanged

        self.prev_close = close
        return self.obv