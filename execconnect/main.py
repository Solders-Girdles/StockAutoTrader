#!/usr/bin/env python3
"""
execconnect/main.py

This production-ready script integrates with RabbitMQ and Postgres.
It consumes approved trades from the "approved_trades" queue, simulates
an order execution (mock/paper trading), and logs the resulting execution
update into a Postgres "trades" table.

Environment Variables:
  - RabbitMQ: RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS
  - Postgres:  DB_HOST, DB_USER, DB_PASS, DB_NAME

In the future, the simulate_paper_trade_execution() function can be replaced
with real API calls (e.g., to Alpaca).
"""

import os
import json
import uuid
import random
import logging
from datetime import datetime
from typing import Dict, Any

import pika
import psycopg2

# ---------------------------
# Logging Configuration
# ---------------------------
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("ExecConnect")

# ---------------------------
# Environment Variables
# ---------------------------
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "")
DB_NAME = os.environ.get("DB_NAME", "trading")


# ---------------------------
# Trade Execution Simulation
# ---------------------------
def simulate_paper_trade_execution(trade: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simulate a paper trade execution.

    In production, replace this function with a real trading API call.

    Parameters:
      trade (dict): Must include:
          - "approved": bool,
          - "symbol": str,
          - "action": str,
          - "price": float,
          - "quantity": int.
        Optional:
          - "stop_loss_price": float,
          - "scenario": str (one of "full", "partial", or "reject"; defaults to "full").

    Returns:
      dict: Execution update with:
          - "order_id": str,
          - "status": str ("FILLED", "PARTIALLY_FILLED", or "REJECTED"),
          - "filled_quantity": int,
          - "avg_price": float,
          - "timestamp": str (ISO format),
          - "remaining_quantity": int (if applicable).
    """
    required_fields = ['approved', 'symbol', 'action', 'price', 'quantity']
    for field in required_fields:
        if field not in trade:
            logger.error("Missing required field '%s' in trade: %s", field, trade)
            raise ValueError(f"Missing required field: {field}")

    if not trade.get("approved", False):
        # Trade not approved; immediately return a rejected update.
        return {
            "order_id": "N/A",
            "status": "REJECTED",
            "filled_quantity": 0,
            "avg_price": 0.0,
            "timestamp": datetime.utcnow().isoformat()
        }

    scenario = trade.get("scenario", "full").lower()
    order_id = f"MOCK-{uuid.uuid4()}"
    timestamp = datetime.utcnow().isoformat()
    symbol = trade.get("symbol", "UNKNOWN")
    quantity = trade.get("quantity", 0)
    requested_price = trade.get("price", 0.0)

    if scenario == "full":
        execution_update = {
            "order_id": order_id,
            "status": "FILLED",
            "filled_quantity": quantity,
            "avg_price": requested_price,
            "timestamp": timestamp
        }
    elif scenario == "partial":
        if quantity > 1:
            filled_qty = random.randint(1, quantity - 1)
        else:
            filled_qty = quantity
            scenario = "full"
        execution_update = {
            "order_id": order_id,
            "status": "PARTIALLY_FILLED" if scenario == "partial" else "FILLED",
            "filled_quantity": filled_qty,
            "avg_price": requested_price + 0.05,  # simulate slight slippage
            "timestamp": timestamp,
            "remaining_quantity": quantity - filled_qty if scenario == "partial" else 0
        }
    elif scenario == "reject":
        execution_update = {
            "order_id": order_id,
            "status": "REJECTED",
            "filled_quantity": 0,
            "avg_price": 0.0,
            "timestamp": timestamp
        }
    else:
        logger.warning("Unknown scenario '%s'. Defaulting to full fill.", scenario)
        execution_update = {
            "order_id": order_id,
            "status": "FILLED",
            "filled_quantity": quantity,
            "avg_price": requested_price,
            "timestamp": timestamp
        }

    return execution_update


def place_order(trade: Dict[str, Any]) -> Dict[str, Any]:
    """
    High-level function to place an order (mock implementation).

    Parameters:
      trade (dict): An approved trade dictionary.

    Returns:
      dict: The execution update.
    """
    try:
        execution_result = simulate_paper_trade_execution(trade)
        logger.info("Execution Update for symbol=%s: %s", trade.get('symbol'), execution_result)
        return execution_result
    except Exception as e:
        logger.error("Error during order placement: %s", str(e))
        raise


# ---------------------------
# Postgres Integration
# ---------------------------
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
        logger.info("Connected to Postgres at %s", DB_HOST)
        return conn
    except Exception as e:
        logger.error("Failed to connect to Postgres: %s", str(e))
        raise


def init_db(conn):
    """
    Initialize the 'trades' table if it does not exist.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS trades (
        order_id TEXT PRIMARY KEY,
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
    logger.info("Database initialized; 'trades' table ensured.")


def insert_trade_update(conn, trade_update: Dict[str, Any]) -> None:
    """
    Insert an execution update into the 'trades' table.

    Parameters:
      conn: psycopg2 connection object.
      trade_update (dict): Execution update dictionary.
    """
    insert_query = """
    INSERT INTO trades (order_id, symbol, status, filled_quantity, avg_price, execution_timestamp, remaining_quantity)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (order_id) DO NOTHING;
    """
    # Attempt to extract symbol from the original trade if available,
    # otherwise, fall back to the trade_update contents.
    symbol = trade_update.get("trade", {}).get("symbol") or trade_update.get("symbol", "UNKNOWN")
    order_id = trade_update.get("order_id")
    status = trade_update.get("status")
    filled_quantity = trade_update.get("filled_quantity")
    avg_price = trade_update.get("avg_price")
    execution_timestamp = trade_update.get("timestamp")
    remaining_quantity = trade_update.get("remaining_quantity", 0)

    with conn.cursor() as cur:
        cur.execute(insert_query,
                    (order_id, symbol, status, filled_quantity, avg_price, execution_timestamp, remaining_quantity))
        conn.commit()
    logger.info("Inserted trade update into DB with order_id: %s", order_id)


# ---------------------------
# RabbitMQ Consumer
# ---------------------------
def process_message(ch, method, properties, body, db_conn):
    """
    Callback function to process messages from RabbitMQ.

    Parameters:
      ch: RabbitMQ channel.
      method: Delivery method.
      properties: Message properties.
      body: The message body.
      db_conn: Postgres connection object.
    """
    try:
        message = json.loads(body.decode())
        logger.info("Received approved trade: %s", message)

        # Process the trade: simulate order execution.
        execution_update = place_order(message)

        # Log execution update in Postgres.
        insert_trade_update(db_conn, execution_update)

        # Acknowledge the message.
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error("Error processing message: %s", str(e))
        # Optionally: reject and do not requeue the message.
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    # Connect to Postgres and ensure the trades table exists.
    db_conn = get_db_connection()
    init_db(db_conn)

    # Set up RabbitMQ connection.
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # Declare the queue to ensure it exists.
    queue_name = "approved_trades"
    channel.queue_declare(queue=queue_name, durable=True)

    logger.info("Waiting for messages in queue '%s'. To exit press CTRL+C", queue_name)

    # Start consuming messages.
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=lambda ch, method, properties, body: process_message(ch, method, properties, body, db_conn)
    )

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down.")
    finally:
        channel.stop_consuming()
        connection.close()
        db_conn.close()


if __name__ == "__main__":
    main()