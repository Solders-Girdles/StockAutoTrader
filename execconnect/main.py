#!/usr/bin/env python3
"""
main.py

This production-ready script integrates with RabbitMQ and Postgres to process approved trades.
It consumes messages from the "approved_trades" queue, executes trades using the trade_executor module,
logs each execution event, and stores the results (including order state changes) in Postgres.
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
    The table now includes an 'order_state' column to track the order lifecycle.
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
        remaining_quantity INTEGER,
        order_state TEXT
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

def insert_trade_update(conn, trade_event: Dict[str, Any]) -> None:
    """
    Insert a trade execution event into the 'trades' table.
    Each event (e.g., pending, partial fill, final fill) is stored separately.
    """
    insert_query = """
    INSERT INTO trades (order_id, symbol, status, filled_quantity, avg_price, execution_timestamp, remaining_quantity, order_state)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    symbol = trade_event.get("symbol", "UNKNOWN")
    order_id = trade_event.get("order_id")
    status = trade_event.get("status")
    filled_quantity = trade_event.get("filled_quantity")
    avg_price = trade_event.get("avg_price")
    execution_timestamp = trade_event.get("timestamp")
    remaining_quantity = trade_event.get("remaining_quantity", 0)
    order_state = trade_event.get("order_state", status)
    with conn.cursor() as cur:
        cur.execute(insert_query, (order_id, symbol, status, filled_quantity, avg_price, execution_timestamp, remaining_quantity, order_state))
        conn.commit()
    log_msg = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": "INFO",
        "service": "ExecConnect.Main",
        "message": f"Inserted trade event into DB with order_id: {order_id}",
        "order_state": order_state
    }
    logger.info(json.dumps(log_msg))

def process_message(ch, method, properties, body, db_conn):
    """
    Callback to process messages from RabbitMQ.
    Consumes an approved trade, executes the trade (logging pending and fill events),
    and stores each event in Postgres.
    """
    try:
        message = json.loads(body.decode())
        logger.info(json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "level": "INFO",
            "service": "ExecConnect.Main",
            "message": "Received approved trade",
            "trade": message
        }))
        execution_events = execute_trade(message)
        for event in execution_events:
            log_trade_execution(event)
            insert_trade_update(db_conn, event)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "level": "ERROR",
            "service": "ExecConnect.Main",
            "message": f"Error processing message: {str(e)}",
            "stack_trace": traceback.format_exc()
        }))
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
    logger.info(json.dumps({
        "timestamp": datetime.utcnow().isoformat(),
        "level": "INFO",
        "service": "ExecConnect.Main",
        "message": f"Waiting for messages in queue '{queue_name}'."
    }))
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=lambda ch, method, props, body: process_message(ch, method, props, body, db_conn)
    )
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info(json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "level": "INFO",
            "service": "ExecConnect.Main",
            "message": "Interrupted by user, shutting down."
        }))
    finally:
        channel.stop_consuming()
        connection.close()
        db_conn.close()

if __name__ == "__main__":
    main()