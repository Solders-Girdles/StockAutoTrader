#!/usr/bin/env python3
"""
mq_util.py

RabbitMQ Communication Utilities:
- Provides helper functions for connecting and publishing messages with retry logic and robust error handling.
- All log entries include explicit JSON fields (timestamp, level, service, message, and optional correlation_id).
"""

import os
import json
import time
import logging
import pika
import pika.exceptions
from datetime import datetime, timezone

logger = logging.getLogger("MQUtil")
logger.setLevel(logging.INFO)

def _json_log(level: str, service: str, message: str, extra: dict = None):
    log = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "service": service,
        "message": message
    }
    if extra:
        log.update(extra)
    return log

def get_rabbitmq_parameters():
    rabbitmq_host = os.environ.get("RABBITMQ_HOST", "localhost")
    rabbitmq_user = os.environ.get("RABBITMQ_USER", "guest")
    rabbitmq_pass = os.environ.get("RABBITMQ_PASS", "guest")
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
    return pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials)

def publish_message(queue_name: str, message: dict, max_retries: int = 5, initial_delay: int = 1, correlation_id: str = None) -> None:
    """
    Publish a message to the specified RabbitMQ queue with retry logic.

    Args:
        queue_name (str): Name of the RabbitMQ queue.
        message (dict): The message to publish.
        max_retries (int): Maximum number of retries.
        initial_delay (int): Initial delay (in seconds) before retrying.
        correlation_id (str): Optional correlation identifier.
    """
    parameters = get_rabbitmq_parameters()
    retry_delay = initial_delay

    for attempt in range(1, max_retries + 1):
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
            message_json = json.dumps(message)
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message_json,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            logger.info(json.dumps(_json_log(
                level="INFO",
                service="RiskOps",
                message="Message published successfully.",
                extra={"queue": queue_name, "attempt": attempt, "message": message, "correlation_id": correlation_id}
            )))
            connection.close()
            return
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
            logger.error(json.dumps(_json_log(
                level="ERROR",
                service="RiskOps",
                message="Publish retry due to RabbitMQ connection error.",
                extra={"queue": queue_name, "attempt": attempt, "error": str(e), "retry_delay": retry_delay, "correlation_id": correlation_id}
            )))
            time.sleep(retry_delay)
            retry_delay *= 2
        except Exception as e:
            logger.exception(json.dumps(_json_log(
                level="ERROR",
                service="RiskOps",
                message="Unexpected error during publish attempt.",
                extra={"queue": queue_name, "attempt": attempt, "error": str(e), "correlation_id": correlation_id}
            )))
            time.sleep(retry_delay)
            retry_delay *= 2

    logger.error(json.dumps(_json_log(
        level="ERROR",
        service="RiskOps",
        message="Failed to publish message after maximum retries.",
        extra={"queue": queue_name, "max_retries": max_retries, "message": message, "correlation_id": correlation_id}
    )))