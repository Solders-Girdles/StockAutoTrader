import logging
import json
from datetime import datetime, timezone

class JsonFormatter(logging.Formatter):
    def format(self, record):
        # Build a dictionary for the JSON log record.
        log_record = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'service': getattr(record, 'service', 'UnknownService'),
            'level': record.levelname,
            'message': record.getMessage(),
        }
        # Merge in any extra fields (exclude common LogRecord attributes)
        standard_keys = {
            'name', 'msg', 'args', 'levelname', 'levelno', 'pathname',
            'filename', 'module', 'exc_info', 'exc_text', 'stack_info',
            'lineno', 'funcName', 'created', 'msecs', 'relativeCreated',
            'thread', 'threadName', 'processName', 'process'
        }
        for key, value in record.__dict__.items():
            if key not in standard_keys:
                log_record[key] = value
        return json.dumps(log_record)

class JsonLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        # Ensure that self.extra is merged into kwargs['extra'].
        extra = kwargs.setdefault("extra", {})
        extra.update(self.extra)
        return msg, kwargs

def get_logger(service_name: str) -> logging.Logger:
    logger = logging.getLogger(service_name)
    logger.setLevel(logging.INFO)
    # If no handlers are attached, add one with our JSON formatter.
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    return JsonLoggerAdapter(logger, {'service': service_name})

# --- Added wrapper function ---
def log_json(level, message, extra=None):
    """
    Logs a message at the specified level using a JSON-formatted logger.
    """
    import logging
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logger = get_logger("Quant")
    logger.log(numeric_level, message, extra=extra or {})