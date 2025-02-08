#!/usr/bin/env python3
"""
riskops/main.py

Production-Ready RiskOps Module:
- Consumes trade signals from RabbitMQ "trade_signals" queue.
- Applies a configurable risk check (default 2% of portfolio capital).
- Updates a Postgres portfolio (tracking total_capital and positions).
- Publishes approved trades to RabbitMQ "approved_trades" queue.
- Environment variables:
    RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS
    DB_HOST, DB_USER, DB_PASS, DB_NAME
"""

import os
import json
import math
import logging
import pika
import psycopg2
import psycopg2.extras
from dataclasses import dataclass, field
from typing import Dict, Optional

# -------------------------------------------------------------------
# Logging Configuration
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("RiskOps")

# -------------------------------------------------------------------
# Environment Variables and Constants
# -------------------------------------------------------------------
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "postgres")
DB_NAME = os.environ.get("DB_NAME", "riskops_db")

TRADE_SIGNALS_QUEUE = "trade_signals"
APPROVED_TRADES_QUEUE = "approved_trades"

MAX_RISK_PER_TRADE = 0.02  # 2% risk per trade
STOP_LOSS_BUFFER = 0.05    # 5% stop-loss buffer for BUY orders
INITIAL_CAPITAL = 100000.00

# -------------------------------------------------------------------
# Data Classes for Trade Signals and Results
# -------------------------------------------------------------------
@dataclass
class TradeSignal:
    symbol: str
    action: str  # "BUY" or "SELL"
    price: float
    desired_quantity: Optional[int] = None

@dataclass
class TradeResult:
    approved: bool
    symbol: str
    action: str
    executed_quantity: int = 0
    stop_loss: Optional[float] = None
    reason: Optional[str] = None

# -------------------------------------------------------------------
# PortfolioDB: Manages the Postgres Portfolio Table
# -------------------------------------------------------------------
class PortfolioDB:
    def __init__(self, conn):
        self.conn = conn
        self.ensure_table()

    def ensure_table(self):
        """Creates the portfolio table if it doesn't exist and initializes a record."""
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS portfolio (
                    id SERIAL PRIMARY KEY,
                    total_capital NUMERIC NOT NULL,
                    positions JSONB DEFAULT '{}'::jsonb
                );
            """)
            self.conn.commit()
            # Ensure a single portfolio record exists (assumed with id=1)
            cur.execute("SELECT COUNT(*) FROM portfolio;")
            count = cur.fetchone()[0]
            if count == 0:
                cur.execute(
                    "INSERT INTO portfolio (total_capital, positions) VALUES (%s, %s);",
                    (INITIAL_CAPITAL, json.dumps({}))
                )
                self.conn.commit()
                logger.info("Initialized portfolio with capital: %s", INITIAL_CAPITAL)

    def get_portfolio(self) -> Dict:
        """Fetches the portfolio record (assumes a single row with id=1)."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM portfolio WHERE id = 1;")
            portfolio = cur.fetchone()
            return portfolio

    def update_portfolio_buy(self, symbol: str, quantity: int, price: float):
        """Updates the portfolio for a BUY trade."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            portfolio = self.get_portfolio()
            total_capital = float(portfolio["total_capital"])
            positions = portfolio["positions"] or {}
            cost = quantity * price
            if cost > total_capital:
                raise ValueError("Insufficient capital in portfolio.")
            total_capital -= cost

            # Update positions: calculate new weighted average price if position exists.
            if symbol in positions:
                pos = positions[symbol]
                old_shares = pos.get("shares", 0)
                old_avg = pos.get("avg_price", 0.0)
                new_shares = old_shares + quantity
                new_avg = ((old_shares * old_avg) + (quantity * price)) / new_shares
                positions[symbol] = {"shares": new_shares, "avg_price": new_avg}
            else:
                positions[symbol] = {"shares": quantity, "avg_price": price}

            cur.execute(
                "UPDATE portfolio SET total_capital = %s, positions = %s WHERE id = 1;",
                (total_capital, json.dumps(positions))
            )
            self.conn.commit()
            logger.info("Portfolio BUY updated: %s shares of %s at $%s", quantity, symbol, price)

    def update_portfolio_sell(self, symbol: str, quantity: int, price: float):
        """Updates the portfolio for a SELL trade."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            portfolio = self.get_portfolio()
            total_capital = float(portfolio["total_capital"])
            positions = portfolio["positions"] or {}
            if symbol not in positions or positions[symbol].get("shares", 0) < quantity:
                raise ValueError("Insufficient shares to sell.")
            proceeds = quantity * price
            total_capital += proceeds
            pos = positions[symbol]
            pos["shares"] -= quantity
            if pos["shares"] <= 0:
                del positions[symbol]
            else:
                positions[symbol] = pos

            cur.execute(
                "UPDATE portfolio SET total_capital = %s, positions = %s WHERE id = 1;",
                (total_capital, json.dumps(positions))
            )
            self.conn.commit()
            logger.info("Portfolio SELL updated: %s shares of %s at $%s", quantity, symbol, price)

# -------------------------------------------------------------------
# RiskManager: Applies Risk Constraints and Updates the Portfolio
# -------------------------------------------------------------------
class RiskManager:
    def __init__(self, portfolio_db: PortfolioDB,
                 max_risk_per_trade: float = MAX_RISK_PER_TRADE,
                 stop_loss_buffer: float = STOP_LOSS_BUFFER):
        self.portfolio_db = portfolio_db
        self.max_risk_per_trade = max_risk_per_trade
        self.stop_loss_buffer = stop_loss_buffer

    def process_trade_signal(self, signal: TradeSignal) -> TradeResult:
        symbol = signal.symbol
        action = signal.action.upper()
        price = signal.price

        # Validate the signal.
        if not symbol or action not in {"BUY", "SELL"} or price <= 0:
            reason = "Invalid trade signal data."
            logger.error(f"{reason} Signal: {signal}")
            return TradeResult(approved=False, symbol=symbol, action=action, reason=reason)

        portfolio = self.portfolio_db.get_portfolio()
        total_capital = float(portfolio["total_capital"])
        max_notional = total_capital * self.max_risk_per_trade

        # For BUY orders, set a stop-loss and compute risk per share.
        if action == "BUY":
            stop_loss_price = round(price * (1 - self.stop_loss_buffer), 2)
            risk_per_share = price - stop_loss_price
            if risk_per_share <= 0:
                reason = "Computed non-positive risk per share."
                logger.error(reason)
                return TradeResult(approved=False, symbol=symbol, action=action, reason=reason)
            max_shares_by_risk = math.floor(max_notional / risk_per_share)
        elif action == "SELL":
            stop_loss_price = None
            risk_per_share = 0  # Not used for SELL orders.
        else:
            reason = f"Unknown action: {action}"
            logger.error(reason)
            return TradeResult(approved=False, symbol=symbol, action=action, reason=reason)

        # Determine final quantity: for BUY, use the lower of the risk-adjusted or desired quantity.
        if signal.desired_quantity is not None:
            quantity = (min(signal.desired_quantity, max_shares_by_risk)
                        if action == "BUY" else signal.desired_quantity)
        else:
            quantity = max_shares_by_risk if action == "BUY" else 0

        if action == "BUY" and (quantity is None or quantity < 1):
            reason = "Risk constraints prevent executing even 1 share."
            logger.info(reason)
            return TradeResult(approved=False, symbol=symbol, action=action, reason=reason)

        if action == "SELL":
            positions = portfolio.get("positions") or {}
            held = positions.get(symbol, {}).get("shares", 0)
            if held < quantity:
                reason = f"Insufficient shares to sell. Held: {held}, Requested: {quantity}"
                logger.error(reason)
                return TradeResult(approved=False, symbol=symbol, action=action, reason=reason)

        try:
            if action == "BUY":
                total_cost = quantity * price
                if total_cost > total_capital:
                    reason = "Not enough capital for BUY trade."
                    logger.error(reason)
                    return TradeResult(approved=False, symbol=symbol, action=action, reason=reason)
                self.portfolio_db.update_portfolio_buy(symbol, quantity, price)
                logger.info("BUY executed: %s shares of %s at $%s. Stop-loss: $%s", quantity, symbol, price, stop_loss_price)
                return TradeResult(approved=True, symbol=symbol, action=action, executed_quantity=quantity, stop_loss=stop_loss_price)
            elif action == "SELL":
                self.portfolio_db.update_portfolio_sell(symbol, quantity, price)
                logger.info("SELL executed: %s shares of %s at $%s.", quantity, symbol, price)
                return TradeResult(approved=True, symbol=symbol, action=action, executed_quantity=quantity)
        except Exception as e:
            reason = f"Trade execution error: {e}"
            logger.exception(reason)
            return TradeResult(approved=False, symbol=symbol, action=action, reason=reason)

        reason = "Unhandled trade action."
        logger.error(reason)
        return TradeResult(approved=False, symbol=symbol, action=action, reason=reason)

# -------------------------------------------------------------------
# RabbitMQ Connection and Message Handling
# -------------------------------------------------------------------
def setup_rabbitmq():
    """Establishes and returns a RabbitMQ connection and channel."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=TRADE_SIGNALS_QUEUE, durable=True)
    channel.queue_declare(queue=APPROVED_TRADES_QUEUE, durable=True)
    return connection, channel

def publish_approved_trade(channel, trade_result: TradeResult):
    """Publishes an approved trade to the 'approved_trades' queue."""
    message = json.dumps({
        "symbol": trade_result.symbol,
        "action": trade_result.action,
        "executed_quantity": trade_result.executed_quantity,
        "stop_loss": trade_result.stop_loss
    })
    channel.basic_publish(
        exchange='',
        routing_key=APPROVED_TRADES_QUEUE,
        body=message,
        properties=pika.BasicProperties(delivery_mode=2)  # persistent message
    )
    logger.info("Published approved trade: %s", message)

def on_message(channel, method_frame, header_frame, body, risk_manager: RiskManager):
    """Callback invoked when a trade signal message is received."""
    try:
        message = json.loads(body)
        logger.info("Received trade signal: %s", message)
        signal = TradeSignal(
            symbol=message.get("symbol"),
            action=message.get("action"),
            price=float(message.get("price")),
            desired_quantity=message.get("desired_quantity")
        )
        result = risk_manager.process_trade_signal(signal)
        if result.approved:
            publish_approved_trade(channel, result)
        else:
            logger.info("Trade signal rejected: %s", result.reason)
    except Exception as e:
        logger.exception("Failed to process message: %s", e)
    finally:
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

def start_consuming(risk_manager: RiskManager):
    """Starts consuming messages from the 'trade_signals' queue."""
    connection, channel = setup_rabbitmq()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=TRADE_SIGNALS_QUEUE,
        on_message_callback=lambda ch, method, properties, body: on_message(ch, method, properties, body, risk_manager)
    )
    logger.info("Started consuming from queue: %s", TRADE_SIGNALS_QUEUE)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()

# -------------------------------------------------------------------
# Main Entry Point
# -------------------------------------------------------------------
def main():
    # Connect to Postgres.
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            dbname=DB_NAME
        )
        logger.info("Connected to Postgres database: %s", DB_NAME)
    except Exception as e:
        logger.exception("Failed to connect to Postgres: %s", e)
        return

    # Initialize PortfolioDB and RiskManager.
    portfolio_db = PortfolioDB(conn)
    risk_manager = RiskManager(portfolio_db)

    # Start consuming trade signals from RabbitMQ.
    start_consuming(risk_manager)

if __name__ == "__main__":
    main()