import unittest
import uuid
import json
import logging
from common.logging_helper import get_logger, JsonFormatter

class CaptureJsonLogs:
    """
    A context manager to capture logs emitted by a logger using our JSON formatter.
    """
    def __init__(self, logger_name, level="INFO"):
        self.logger_name = logger_name
        self.level = level
        self.records = []
        self.handler = None

    def __enter__(self):
        self.logger = logging.getLogger(self.logger_name)
        self.original_handlers = self.logger.handlers[:]
        self.logger.handlers = []
        self.handler = logging.StreamHandler(self)
        self.handler.setFormatter(JsonFormatter())
        self.logger.addHandler(self.handler)
        self.logger.setLevel(getattr(logging, self.level.upper(), self.level))
        return self

    def write(self, message):
        for line in message.splitlines():
            if line.strip():
                self.records.append(line.strip())

    def flush(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        self.logger.removeHandler(self.handler)
        self.logger.handlers = self.original_handlers

class TestLoggingIntegration(unittest.TestCase):
    def test_log_contains_correlation_id(self):
        test_correlation_id = str(uuid.uuid4())
        logger = get_logger("TestService")
        with CaptureJsonLogs("TestService", level="INFO") as cap:
            logger.info("Testing logging integration", extra={'correlation_id': test_correlation_id})
        found = any(
            json.loads(line).get("correlation_id") == test_correlation_id
            for line in cap.records
        )
        self.assertTrue(found, f"Log output did not contain the correlation ID: {test_correlation_id}")

if __name__ == "__main__":
    unittest.main()