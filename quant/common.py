# common.py
import os
import json
import time
import traceback
from datetime import datetime
from logging_helper import log_json
import pika

def process_market_data(data: dict) -> dict:
    if "symbol" not in data or not data.get("symbol") or not str(data["symbol"]).strip():
        err = {"error": "Missing or empty required field: 'symbol'"}
        log_json("ERROR", "Data validation error", extra=err)
        raise ValueError(err)
    if "price" not in data:
        err = {"error": "Missing required field: 'price'"}
        log_json("ERROR", "Data validation error", extra=err)
        raise ValueError(err)
    try:
        symbol = str(data["symbol"]).strip()
        price = float(data["price"])
    except (ValueError, TypeError) as e:
        log_json("ERROR", "Invalid type for 'symbol' or 'price'", extra={
            "exception": str(e),
            "stack_trace": traceback.format_exc()
        })
        raise
    timestamp = data.get("timestamp") or datetime.utcnow().isoformat() + "Z"
    volume = data.get("volume")
    if volume is None:
        volume = 0
        log_json("WARNING", "Missing volume; defaulting to 0")
    else:
        try:
            volume = float(volume)
        except (ValueError, TypeError) as e:
            log_json("ERROR", "Invalid type for 'volume'", extra={
                "exception": str(e),
                "stack_trace": traceback.format_exc()
            })
            volume = 0
    processed_data = {
        "symbol": symbol,
        "timestamp": timestamp,
        "price": price,
        "volume": volume
    }
    log_json("INFO", "Processed market data", extra={"data": processed_data})
    return processed_data

def compute_signal(data: dict) -> dict:
    max_retries = 3
    import random
    from datetime import datetime
    for attempt in range(1, max_retries + 1):
        try:
            price = data["price"]
            signal_val = "BUY" if price < 125 else "SELL"
            signal = {
                "symbol": data["symbol"],
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "signal_type": signal_val,
                "action": signal_val,
                "signal": signal_val,
                "price": price,
                "confidence": round(random.uniform(0.7, 1.0), 2),
                "source": "fallback"
            }
            log_json("INFO", "Computed fallback signal", extra={"signal": signal})
            return signal
        except KeyError as ke:
            log_json("ERROR", "Missing key during signal computation", extra={
                "exception": str(ke),
                "stack_trace": traceback.format_exc(),
                "attempt": attempt
            })
            break
        except Exception as e:
            log_json("ERROR", "Error computing signal", extra={
                "exception": str(e),
                "stack_trace": traceback.format_exc(),
                "attempt": attempt
            })
            time.sleep(0.5 * attempt)
    fallback_signal = {
        "symbol": data.get("symbol", "UNKNOWN"),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "signal_type": "HOLD",
        "action": "HOLD",
        "signal": "HOLD",
        "price": data.get("price", 0.0),
        "confidence": 0.0,
        "source": "fallback"
    }
    log_json("INFO", "Returning fallback signal", extra={"signal": fallback_signal})
    return fallback_signal

def publish_trade_signal(signal: dict) -> None:
    import json
    import time
    import pika
    rabbitmq_host = os.environ.get("RABBITMQ_HOST", "localhost")
    rabbitmq_user = os.environ.get("RABBITMQ_USER", "guest")
    rabbitmq_pass = os.environ.get("RABBITMQ_PASS", "guest")
    message = json.dumps(signal)
    max_retries = 3
    delay = 1
    for attempt in range(1, max_retries + 1):
        try:
            credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
            parameters = pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials)
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue="trade_signals", durable=True)
            channel.basic_publish(
                exchange="",
                routing_key="trade_signals",
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            log_json("INFO", "Published trade signal", extra={"signal": signal})
            connection.close()
            return
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
            log_json("ERROR", "RabbitMQ network error while publishing signal", extra={
                "exception": str(e),
                "stack_trace": traceback.format_exc(),
                "attempt": attempt
            })
        except Exception as e:
            log_json("ERROR", "Unexpected error while publishing trade signal", extra={
                "exception": str(e),
                "stack_trace": traceback.format_exc(),
                "attempt": attempt
            })
        time.sleep(delay)
        delay *= 2
    log_json("ERROR", "Failed to publish trade signal after retries", extra={"signal": signal})

def get_rabbitmq_connection() -> pika.BlockingConnection:
    rabbitmq_host = os.environ.get("RABBITMQ_HOST", "localhost")
    rabbitmq_user = os.environ.get("RABBITMQ_USER", "guest")
    rabbitmq_pass = os.environ.get("RABBITMQ_PASS", "guest")
    import pika
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
    parameters = pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials)
    attempt = 0
    delay = 2
    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            log_json("INFO", "Successfully connected to RabbitMQ", extra={"host": rabbitmq_host})
            return connection
        except Exception as e:
            attempt += 1
            log_json("ERROR", "RabbitMQ connection refused", extra={
                "exception": str(e),
                "stack_trace": traceback.format_exc(),
                "attempt": attempt
            })
            time.sleep(delay)
            delay *= 2