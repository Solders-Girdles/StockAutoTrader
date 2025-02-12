# common/schema.py
data_schema = {
    "type": "object",
    "properties": {
        "ev": {"type": "string"},       # Event Type
        "sym": {"type": "string"},      # Symbol
        "x": {"type": "integer"},      # Exchange ID
        "i": {"type": "string"},       # Trade ID
        "z": {"type": "integer"},       # Tape (where trade happened)
        "p": {"type": "number"},      # Price
        "s": {"type": "integer"},      # Size (volume)
        "c": {"type": "array", "items": {"type": "integer"}},  # Conditions
        "t": {"type": "integer"},       # Timestamp (nanoseconds)
        "q": {"type": "integer"},        #Sequence number
        #Added additional fields for other event types
        "bp": {"type": "number"}, #bid price
        "ap": {"type": "number"}, #ask price
        "as": {"type": "integer"}, #ask size
        "bs": {"type": "integer"}, # bid size
        "op": {"type": "number"}, # open price
        "cl": {"type": "number"}, # close price
        "h": {"type": "number"},  # high price
        "l": {"type": "number"},   # low price
        "v": {"type": "integer"},   # volume
        "vw": {"type": "number"}, # weighted volume
        "a": {"type": "number"}, # average price
        "z": {"type": "number"}, # average trade size
    },
    "required": ["ev", "sym", "t"]  # Basic required fields
}