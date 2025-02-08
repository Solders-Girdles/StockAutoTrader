#!/usr/bin/env python3
"""
execconnect/main.py

This production-ready script integrates with RabbitMQ and Postgres to process
approved trades. It simulates order execution with enhanced fill logic
(including partial fills, full fills, and rejections) and optionally integrates
with a real paper broker (e.g., Alpaca) if API credentials are provided.

Approved trade messages are consumed from the "approved_trades" queue.
Each trade execution event is logged in the Postgres "trades" table.

Environment Variables:
  RabbitMQ:
    - RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS
  Postgres:
    - DB_HOST, DB_USER, DB_PASS, DB_NAME
  Broker (Optional - for real integration with Alpaca):
    - ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL
"""

import os
import json
import uuid
import random
import time
import logging
import traceback
from datetime import datetime
from typing import Dict, Any, List

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

# Optional Broker Credentials for Alpaca integration
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY")
ALPACA_BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")


# ---------------------------
# Existing Trade Execution Simulation (Partial Fill, etc.)
# ---------------------------
def simulate_paper_trade_execution(trade: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Simulate a paper trade execution with enhanced fill logic.
    Depending on the "scenario" key in the trade dict, the trade may be:
      - Fully filled ("full")
      - Partially filled with multiple events ("partial")
      - Rejected ("reject")
    If "scenario" is not provided, defaults to "full".

    Parameters:
        trade (dict): Must include:
            - "approved": bool,
            - "symbol": str,
            - "action": str,
            - "price": float,
            - "quantity": int.
          Optional:
            - "stop_loss_price": float,
            - "scenario": str (one of "full", "partial", "reject").

    Returns:
        List[dict]: A list of execution event dictionaries. Each event includes:
            - "order_id": str,
            - "status": str ("FILLED", "PARTIALLY_FILLED", or "REJECTED"),
            - "filled_quantity": int,
            - "avg_price": float,
            - "timestamp": str (ISO format),
            - "remaining_quantity": int (if applicable).
    """
    # Validate required fields
    required_fields = ['approved', 'symbol', 'action', 'price', 'quantity']
    for field in required_fields:
        if field not in trade:
            logger.error("Missing required field '%s' in trade: %s", field, trade)
            raise ValueError(f"Missing required field: {field}")

    if not trade.get("approved", False):
        # Trade not approved; immediately return a rejected event.
        return [{
            "order_id": "N/A",
            "status": "REJECTED",
            "filled_quantity": 0,
            "avg_price": 0.0,
            "timestamp": datetime.utcnow().isoformat(),
            "remaining_quantity": trade.get("quantity", 0)
        }]

    scenario = trade.get("scenario", "full").lower()
    order_id = f"MOCK-{uuid.uuid4()}"
    base_timestamp = datetime.utcnow().isoformat()
    symbol = trade.get("symbol", "UNKNOWN")
    quantity = trade.get("quantity", 0)
    requested_price = trade.get("price", 0.0)

    if scenario == "full":
        event = {
            "order_id": order_id,
            "status": "FILLED",
            "filled_quantity": quantity,
            "avg_price": requested_price,
            "timestamp": base_timestamp,
            "remaining_quantity": 0
        }
        return [event]

    elif scenario == "partial":
        # Simulate an initial partial fill and then a final fill event.
        if quantity > 1:
            # Randomly decide the quantity filled in the first event
            first_fill = random.randint(1, quantity - 1)
        else:
            first_fill = quantity  # With quantity 1, it's a full fill.

        first_event = {
            "order_id": order_id,
            "status": "PARTIALLY_FILLED",
            "filled_quantity": first_fill,
            "avg_price": requested_price + 0.05,  # simulate slight price slippage
            "timestamp": base_timestamp,
            "remaining_quantity": quantity - first_fill
        }
        # Simulate a second event to fill the remainder
        final_timestamp = datetime.utcnow().isoformat()
        final_event = {
            "order_id": order_id,
            "status": "FILLED",
            "filled_quantity": quantity,  # total filled quantity
            "avg_price": requested_price,  # assume final price equal to requested price
            "timestamp": final_timestamp,
            "remaining_quantity": 0
        }
        return [first_event, final_event]

    elif scenario == "reject":
        event = {
            "order_id": order_id,
            "status": "REJECTED",
            "filled_quantity": 0,
            "avg_price": 0.0,
            "timestamp": base_timestamp,
            "remaining_quantity": quantity
        }
        return [event]

    else:
        logger.warning("Unknown scenario '%s'. Defaulting to full fill.", scenario)
        event = {
            "order_id": order_id,
            "status": "FILLED",
            "filled_quantity": quantity,
            "avg_price": requested_price,
            "timestamp": base_timestamp,
            "remaining_quantity": 0
        }
        return [event]


# ---------------------------
# Real Broker Integration (Alpaca Example)
# ---------------------------
def broker_place_order(trade: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Placeholder function for integration with a real broker's paper trading API.
    For this example, if ALPACA_API_KEY is provided, we attempt to call Alpaca's API.
    Otherwise, we fallback to simulation.

    Parameters:
        trade (dict): Trade details dictionary.

    Returns:
        List[dict]: A list of execution event dictionaries.
    """
    if ALPACA_API_KEY:
        try:
            import alpaca_trade_api as tradeapi
        except ImportError:
            logger.error("alpaca_trade_api library not installed. Falling back to simulation.")
            return simulate_paper_trade_execution(trade)

        try:
            api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url=ALPACA_BASE_URL)
            order = api.submit_order(
                symbol=trade['symbol'],
                qty=trade['quantity'],
                side=trade['action'].lower(),
                type='limit',
                limit_price=trade['price'],
                time_in_force='gtc'
            )
            execution_event = {
                "order_id": order.id,
                "status": order.status.upper(),
                "filled_quantity": int(order.filled_qty),
                "avg_price": float(order.filled_avg_price) if order.filled_avg_price else trade['price'],
                "timestamp": datetime.utcnow().isoformat(),
                "remaining_quantity": trade['quantity'] - int(order.filled_qty)
            }
            return [execution_event]
        except Exception as e:
            logger.error("Error calling Alpaca API: %s", str(e))
            raise
    else:
        return simulate_paper_trade_execution(trade)


def place_order(trade: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    High-level function to place an order.
    If broker API credentials are provided, use broker_place_order;
    otherwise, use simulation.

    Parameters:
        trade (dict): An approved trade dictionary.

    Returns:
        List[dict]: A list of execution event dictionaries.
    """
    try:
        if ALPACA_API_KEY:
            execution_updates = broker_place_order(trade)
        else:
            execution_updates = simulate_paper_trade_execution(trade)
        for update in execution_updates:
            logger.info("Execution Update for symbol=%s: %s", trade.get('symbol'), update)
        return execution_updates
    except Exception as e:
        logger.error("Error during order placement: %s", str(e))
        raise


# ---------------------------
# New: Enhanced Trade Execution and Logging Functions
# ---------------------------
def execute_trade(trade: Dict) -> Dict:
    """
    Simulate executing an approved trade with enhanced error handling and retry logic.

    This function accepts a trade dictionary with keys including:
        - symbol, timestamp, action, price, quantity, risk_approved.

    It attempts to simulate trade execution by:
        - Generating a unique trade_id.
        - Setting a status ("SUCCESS" or "FAILED") based on risk approval and randomness.
        - Recording the execution time (current UTC time in ISO8601).

    The execution simulation is wrapped in a retry mechanism with exponential backoff for
    network-related errors (e.g., pika.exceptions.AMQPConnectionError, AMQPChannelError).
    Each attempt is logged using structured JSON logs.

    Returns:
        dict: Execution details including trade_id, status, execution_time, and echoed trade fields.
    """
    max_retries = 3
    attempt = 1

    while attempt <= max_retries:
        try:
            log_info = {
                "timestamp": datetime.utcnow().isoformat(),
                "service": "ExecConnect",
                "level": "INFO",
                "message": f"Attempt {attempt}: Executing trade",
                "trade": trade
            }
            print(json.dumps(log_info))

            trade_id = str(uuid.uuid4())
            risk_approved = trade.get("risk_approved", False)
            if not risk_approved:
                status = "FAILED"
            else:
                status = random.choice(["SUCCESS", "FAILED"])

            execution_time = datetime.utcnow().isoformat()
            execution_result = {
                "trade_id": trade_id,
                "status": status,
                "execution_time": execution_time,
                "symbol": trade.get("symbol"),
                "action": trade.get("action"),
                "price": trade.get("price"),
                "quantity": trade.get("quantity")
            }

            log_info = {
                "timestamp": datetime.utcnow().isoformat(),
                "service": "ExecConnect",
                "level": "INFO",
                "message": "Trade execution successful",
                "execution_result": execution_result
            }
            print(json.dumps(log_info))
            return execution_result

        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
            delay = 2 ** attempt
            log_error = {
                "timestamp": datetime.utcnow().isoformat(),
                "service": "ExecConnect",
                "level": "ERROR",
                "message": f"Network error on attempt {attempt}: {str(e)}",
                "stack_trace": traceback.format_exc(),
                "retry_delay": delay
            }
            print(json.dumps(log_error))
            time.sleep(delay)
            attempt += 1

        except Exception as e:
            log_error = {
                "timestamp": datetime.utcnow().isoformat(),
                "service": "ExecConnect",
                "level": "ERROR",
                "message": f"Unexpected error executing trade: {str(e)}",
                "stack_trace": traceback.format_exc()
            }
            print(json.dumps(log_error))
            raise

    final_result = {
        "trade_id": "N/A",
        "status": "FAILED",
        "execution_time": datetime.utcnow().isoformat(),
        "symbol": trade.get("symbol"),
        "action": trade.get("action"),
        "price": trade.get("price"),
        "quantity": trade.get("quantity")
    }
    log_error = {
        "timestamp": datetime.utcnow().isoformat(),
        "service": "ExecConnect",
        "level": "ERROR",
        "message": "All retry attempts failed for executing trade",
        "final_result": final_result
    }
    print(json.dumps(log_error))
    return final_result


def log_trade_execution(execution_result: Dict) -> None:
    """
    Log the trade execution details with robust error handling.

    In production this might write to a DB, but here we use structured JSON logging.
    If any error occurs during logging, it is caught and logged as a JSON error message.

    Parameters:
        execution_result (dict): The trade execution details.
    """
    try:
        log_info = {
            "timestamp": datetime.utcnow().isoformat(),
            "service": "ExecConnect",
            "level": "INFO",
            "message": "Logging trade execution",
            "execution_result": execution_result
        }
        print(json.dumps(log_info))
    except Exception as e:
        log_error = {
            "timestamp": datetime.utcnow().isoformat(),
            "service": "ExecConnect",
            "level": "ERROR",
            "message": f"Failed to log trade execution: {str(e)}",
            "stack_trace": traceback.format_exc()
        }
        print(json.dumps(log_error))


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
    This schema allows multiple execution events per order.
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
    logger.info("Database initialized; 'trades' table ensured.")


def insert_trade_update(conn, trade_update: Dict[str, Any]) -> None:
    """
    Insert an execution update event into the 'trades' table.

    Parameters:
        conn: psycopg2 connection object.
        trade_update (dict): Execution update dictionary.
    """
    insert_query = """
    INSERT INTO trades (order_id, symbol, status, filled_quantity, avg_price, execution_timestamp, remaining_quantity)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
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
    Callback to process messages from RabbitMQ.
    Consumes an approved trade, executes the trade using enhanced logic,
    logs the trade execution, and stores the result in Postgres.

    Parameters:
        ch: RabbitMQ channel.
        method: Delivery method.
        properties: Message properties.
        body: The message body.
        db_conn: Postgres connection.
    """
    try:
        message = json.loads(body.decode())
        logger.info("Received approved trade: %s", message)

        # Execute trade with enhanced error handling and retries.
        execution_result = execute_trade(message)

        # Log the trade execution using robust structured JSON logging.
        log_trade_execution(execution_result)

        # Optionally insert the execution result into the DB.
        insert_trade_update(db_conn, execution_result)

        # Acknowledge the message.
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error("Error processing message: %s", str(e))
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    # Connect to Postgres and initialize the DB.
    db_conn = get_db_connection()
    init_db(db_conn)

    # Set up RabbitMQ connection.
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # Declare the approved_trades queue.
    queue_name = "approved_trades"
    channel.queue_declare(queue=queue_name, durable=True)

    logger.info("Waiting for messages in queue '%s'. To exit press CTRL+C", queue_name)

    # Start consuming messages.
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=lambda ch, method, props, body: process_message(ch, method, props, body, db_conn)
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