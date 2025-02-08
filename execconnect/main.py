#!/usr/bin/env python3
"""
main.py

This is the main integration script for ExecConnect. It connects to RabbitMQ and Postgres,
consumes approved trades from the "approved_trades" queue, executes trades using the trade_executor,
logs each execution, and stores the results in Postgres.
"""

import os
import json
import logging
import traceback
from datetime import datetime
from typing import Dict, Any

import pika
import psycopg2

from trade_executor import execute_trade, log_trade_execution

logger = logging.getLogger("ExecConnect.Main")
logger.setLevel(logging.INFO)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "")
DB_NAME = os.environ.get("DB_NAME", "trading")


def get_db_connection():
    """
    Establish a connection to the Postgres database.
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            dbname=DB_NAME
        )
        log_msg = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": "INFO",
            "service": "ExecConnect.Main",
            "message": f"Connected to Postgres at {DB_HOST}"
        }
        logger.info(json.dumps(log_msg))
        return conn
    except Exception as e:
        log_msg = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": "ERROR",
            "service": "ExecConnect.Main",
            "message": f"Failed to connect to Postgres: {str(e)}",
            "stack_trace": traceback.format_exc()
        }
        logger.error(json.dumps(log_msg))
        raise


def init_db(conn):
    """
    Initialize the 'trades' table if it does not exist.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS trades (
        event_id SERIAL PRIMARY KEY,
        order_id TEXT,
        symbol TEXT,
        status TEXT,
        filled_quantity INTEGER,
        avg_price FLOAT,
        execution_timestamp TIMESTAMP,
        remaining_quantity INTEGER
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()
    log_msg = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": "INFO",
        "service": "ExecConnect.Main",
        "message": "Database initialized; 'trades' table ensured."
    }
    logger.info(json.dumps(log_msg))


def insert_trade_update(conn, trade_update: Dict[str, Any]) -> None:
    """
    Insert an execution update event into the 'trades' table.
    """
    insert_query = """
    INSERT INTO trades (order_id, symbol, status, filled_quantity, avg_price, execution_timestamp, remaining_quantity)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    symbol = trade_update.get("symbol", "UNKNOWN")
    order_id = trade_update.get("order_id")
    status = trade_update.get("status")
    filled_quantity = trade_update.get("filled_quantity")
    avg_price = trade_update.get("avg_price")
    execution_timestamp = trade_update.get("timestamp")
    remaining_quantity = trade_update.get("remaining_quantity", 0)
    with conn.cursor() as cur:
        cur.execute(insert_query, (order_id, symbol, status, filled_quantity, avg_price, execution_timestamp, remaining_quantity))
        conn.commit()
    log_msg = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": "INFO",
        "service": "ExecConnect.Main",
        "message": f"Inserted trade update into DB with order_id: {order_id}"
    }
    logger.info(json.dumps(log_msg))


def process_message(ch, method, properties, body, db_conn):
    """
    Callback to process messages from RabbitMQ.
    Consumes an approved trade, executes the trade, logs the execution,
    and stores the result in Postgres.
    """
    try:
        message = json.loads(body.decode())
        log_msg = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": "INFO",
            "service": "ExecConnect.Main",
            "message": "Received approved trade",
            "trade": message
        }
        logger.info(json.dumps(log_msg))
        execution_result = execute_trade(message)
        log_trade_execution(execution_result)
        insert_trade_update(db_conn, execution_result)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        log_msg = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": "ERROR",
            "service": "ExecConnect.Main",
            "message": f"Error processing message: {str(e)}",
            "stack_trace": traceback.format_exc()
        }
        logger.error(json.dumps(log_msg))
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    db_conn = get_db_connection()
    init_db(db_conn)

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    queue_name = "approved_trades"
    channel.queue_declare(queue=queue_name, durable=True)

    log_msg = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": "INFO",
        "service": "ExecConnect.Main",
        "message": f"Waiting for messages in queue '{queue_name}'."
    }
    logger.info(json.dumps(log_msg))

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=lambda ch, method, props, body: process_message(ch, method, props, body, db_conn)
    )

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log_msg = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": "INFO",
            "service": "ExecConnect.Main",
            "message": "Interrupted by user, shutting down."
        }
        logger.info(json.dumps(log_msg))
    finally:
        channel.stop_consuming()
        connection.close()
        db_conn.close()


if __name__ == "__main__":
    main()