# logging_helper.py
import json
from datetime import datetime

SERVICE_NAME = "Quant"

def log_json(level: str, message: str, extra: dict = None):
    """
    Log a JSON-formatted message with standard keys:
      - timestamp: current UTC time in ISO8601 format
      - level: log level (INFO, DEBUG, ERROR, etc.)
      - service: service name
      - message: descriptive message
    Optionally, extra key/value pairs (e.g., correlation_id) can be included.
    """
    if extra is None:
        extra = {}
    log_entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "level": level.upper(),
        "service": SERVICE_NAME,
        "message": message
    }
    log_entry.update(extra)
    # For demonstration purposes we use print. In production, integrate with a proper logging framework.
    print(json.dumps(log_entry))