#!/usr/bin/env python3
"""
riskops/main.py

Enhanced RiskOps Module:
- Consumes trade signals from the RabbitMQ "trade_signals" queue.
- Applies a 2% risk rule with a 5% stop-loss buffer for BUY orders.
- Tracks portfolio total capital, open positions, and daily PnL in Postgres.
- Enforces additional constraints:
    * Maximum open positions limit.
    * Maximum daily loss limit (trading paused if breached).
- Publishes approved trades to "approved_trades" (or a "trading_paused" message).
- Uses the following environment variables:
    RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS
    DB_HOST, DB_USER, DB_PASS, DB_NAME
"""

import os
import json
import math
import logging
from datetime import date
import pika
import psycopg2
import psycopg2.extras
from dataclasses import dataclass
from typing import Optional

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
TRADING_PAUSED_QUEUE = "trading_paused"

MAX_RISK_PER_TRADE = 0.02   # Risk up to 2% of available capital per trade.
STOP_LOSS_BUFFER = 0.05     # 5% stop-loss buffer for BUY orders.
INITIAL_CAPITAL = 100000.00

MAX_OPEN_POSITIONS = 10     # Maximum allowed open positions.
MAX_DAILY_LOSS = -5000.0    # If daily PnL falls below this (i.e. loss exceeds $5000), pause trading.

# -------------------------------------------------------------------
# Data Classes
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
# DBManager: Manages Postgres Tables for Portfolio, Positions, and Daily PnL
# -------------------------------------------------------------------
class DBManager:
    def __init__(self, conn):
        self.conn = conn
        self.ensure_tables()

    def ensure_tables(self):
        """Creates the required tables if they don't exist."""
        with self.conn.cursor() as cur:
            # Portfolio table (single row storing total capital)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS portfolio (
                    id SERIAL PRIMARY KEY,
                    total_capital NUMERIC NOT NULL
                );
            """)
            # Positions table for open positions.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    symbol VARCHAR(10) PRIMARY KEY,
                    shares INTEGER NOT NULL,
                    avg_price NUMERIC NOT NULL
                );
            """)
            # Daily PnL table to track realized PnL per day.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS daily_pnl (
                    day DATE PRIMARY KEY,
                    pnl NUMERIC NOT NULL
                );
            """)
            self.conn.commit()

            # Initialize portfolio if not exists.
            cur.execute("SELECT COUNT(*) FROM portfolio;")
            if cur.fetchone()[0] == 0:
                cur.execute("INSERT INTO portfolio (total_capital) VALUES (%s);", (INITIAL_CAPITAL,))
                self.conn.commit()
                logger.info("Initialized portfolio with capital: %s", INITIAL_CAPITAL)

            # Initialize today's daily pnl if not exists.
            today = date.today()
            cur.execute("SELECT COUNT(*) FROM daily_pnl WHERE day = %s;", (today,))
            if cur.fetchone()[0] == 0:
                cur.execute("INSERT INTO daily_pnl (day, pnl) VALUES (%s, %s);", (today, 0.0))
                self.conn.commit()
                logger.info("Initialized daily pnl for %s.", today)

    def get_portfolio_capital(self) -> float:
        """Fetches the total capital from the portfolio table."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT total_capital FROM portfolio WHERE id = 1;")
            result = cur.fetchone()
            return float(result[0]) if result else 0.0

    def update_portfolio_capital(self, new_capital: float):
        """Updates the portfolio's total capital."""
        with self.conn.cursor() as cur:
            cur.execute("UPDATE portfolio SET total_capital = %s WHERE id = 1;", (new_capital,))
            self.conn.commit()

    def get_position(self, symbol: str):
        """Retrieves the open position for a symbol."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM positions WHERE symbol = %s;", (symbol,))
            return cur.fetchone()

    def get_open_positions_count(self) -> int:
        """Returns the count of open positions."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM positions;")
            return cur.fetchone()[0]

    def update_position_buy(self, symbol: str, quantity: int, price: float):
        """Updates the positions table for a BUY trade."""
        pos = self.get_position(symbol)
        with self.conn.cursor() as cur:
            if pos:
                old_shares = pos["shares"]
                old_avg = float(pos["avg_price"])
                new_shares = old_shares + quantity
                new_avg = ((old_shares * old_avg) + (quantity * price)) / new_shares
                cur.execute("UPDATE positions SET shares = %s, avg_price = %s WHERE symbol = %s;",
                            (new_shares, new_avg, symbol))
            else:
                cur.execute("INSERT INTO positions (symbol, shares, avg_price) VALUES (%s, %s, %s);",
                            (symbol, quantity, price))
            self.conn.commit()

    def update_position_sell(self, symbol: str, quantity: int, price: float) -> float:
        """
        Updates the positions table for a SELL trade.
        Returns the realized PnL for the trade.
        """
        pos = self.get_position(symbol)
        if not pos or pos["shares"] < quantity:
            raise ValueError(f"Insufficient shares to sell for symbol {symbol}.")
        old_shares = pos["shares"]
        avg_price = float(pos["avg_price"])
        realized_pnl = (price - avg_price) * quantity
        new_shares = old_shares - quantity
        with self.conn.cursor() as cur:
            if new_shares <= 0:
                cur.execute("DELETE FROM positions WHERE symbol = %s;", (symbol,))
            else:
                cur.execute("UPDATE positions SET shares = %s WHERE symbol = %s;", (new_shares, symbol))
            self.conn.commit()
        return realized_pnl

    def update_daily_pnl(self, trade_pnl: float):
        """Updates today's daily pnl by adding the realized trade pnl."""
        today = date.today()
        with self.conn.cursor() as cur:
            cur.execute("UPDATE daily_pnl SET pnl = pnl + %s WHERE day = %s;", (trade_pnl, today))
            self.conn.commit()

    def get_daily_pnl(self) -> float:
        """Fetches today's daily pnl."""
        today = date.today()
        with self.conn.cursor() as cur:
            cur.execute("SELECT pnl FROM daily_pnl WHERE day = %s;", (today,))
            result = cur.fetchone()
            return float(result[0]) if result else 0.0

# -------------------------------------------------------------------
# RiskManager: Applies Risk Constraints and Updates the Portfolio
# -------------------------------------------------------------------
class RiskManager:
    def __init__(self, db_manager: DBManager,
                 max_risk_per_trade: float = MAX_RISK_PER_TRADE,
                 stop_loss_buffer: float = STOP_LOSS_BUFFER):
        self.db_manager = db_manager
        self.max_risk_per_trade = max_risk_per_trade
        self.stop_loss_buffer = stop_loss_buffer

    def process_trade_signal(self, signal: TradeSignal) -> TradeResult:
        symbol = signal.symbol
        action = signal.action.upper()
        price = signal.price

        # Validate the trade signal.
        if not symbol or action not in {"BUY", "SELL"} or price <= 0:
            reason = "Invalid trade signal data."
            logger.error(f"{reason} Signal: {signal}")
            return TradeResult(approved=False, symbol=symbol, action=action, reason=reason)

        # Check if the daily loss limit is hit.
        current_daily_pnl = self.db_manager.get_daily_pnl()
        if current_daily_pnl <= MAX_DAILY_LOSS:
            reason = f"Daily loss limit reached: {current_daily_pnl}. Trading paused."
            logger.warning(reason)
            return TradeResult(approved=False, symbol=symbol, action=action, reason=reason)

        # Retrieve current portfolio capital.
        total_capital = self.db_manager.get_portfolio_capital()
        max_notional = total_capital * self.max_risk_per_trade

        # Additional rule: For BUY orders that would open a new position,
        # enforce a maximum open positions limit.
        if action == "BUY":
            pos = self.db_manager.get_position(symbol)
            if not pos:
                open_positions = self.db_manager.get_open_positions_count()
                if open_positions >= MAX_OPEN_POSITIONS:
                    reason = f"Open positions limit reached: {open_positions}."
                    logger.warning(reason)
                    return TradeResult(approved=False, symbol=symbol, action=action, reason=reason)
            stop_loss_price = round(price * (1 - self.stop_loss_buffer), 2)
            risk_per_share = price - stop_loss_price
            if risk_per_share <= 0:
                reason = "Computed non-positive risk per share."
                logger.error(reason)
                return TradeResult(approved=False, symbol=symbol, action=action, reason=reason)
            max_shares_by_risk = math.floor(max_notional / risk_per_share)
        elif action == "SELL":
            stop_loss_price = None
            risk_per_share = 0
        else:
            reason = f"Unknown action: {action}"
            logger.error(reason)
            return TradeResult(approved=False, symbol=symbol, action=action, reason=reason)

        # Determine final trade quantity.
        if signal.desired_quantity is not None:
            quantity = min(signal.desired_quantity, max_shares_by_risk) if action == "BUY" else signal.desired_quantity
        else:
            quantity = max_shares_by_risk if action == "BUY" else 0

        if action == "BUY" and (quantity < 1):
            reason = "Risk constraints prevent executing even 1 share."
            logger.info(reason)
            return TradeResult(approved=False, symbol=symbol, action=action, reason=reason)

        if action == "SELL":
            pos = self.db_manager.get_position(symbol)
            if not pos or pos["shares"] < quantity:
                held = pos["shares"] if pos else 0
                reason = f"Insufficient shares to sell: Held {held}, Requested {quantity}."
                logger.error(reason)
                return TradeResult(approved=False, symbol=symbol, action=action, reason=reason)

        # Execute trade.
        try:
            if action == "BUY":
                total_cost = quantity * price
                if total_cost > total_capital:
                    reason = "Not enough capital for BUY trade."
                    logger.error(reason)
                    return TradeResult(approved=False, symbol=symbol, action=action, reason=reason)
                new_capital = total_capital - total_cost
                self.db_manager.update_portfolio_capital(new_capital)
                self.db_manager.update_position_buy(symbol, quantity, price)
                logger.info("BUY executed: %s shares of %s at $%s. Stop-loss: $%s", quantity, symbol, price, stop_loss_price)
                return TradeResult(approved=True, symbol=symbol, action=action, executed_quantity=quantity, stop_loss=stop_loss_price)
            elif action == "SELL":
                # Execute sell trade and compute realized PnL.
                realized_pnl = self.db_manager.update_position_sell(symbol, quantity, price)
                new_capital = total_capital + (quantity * price)
                self.db_manager.update_portfolio_capital(new_capital)
                self.db_manager.update_daily_pnl(realized_pnl)
                logger.info("SELL executed: %s shares of %s at $%s. Realized PnL: $%s", quantity, symbol, price, realized_pnl)
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
    channel.queue_declare(queue=TRADING_PAUSED_QUEUE, durable=True)
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
        properties=pika.BasicProperties(delivery_mode=2)
    )
    logger.info("Published approved trade: %s", message)

def publish_trading_paused(channel, reason: str):
    """Publishes a trading_paused message to notify downstream systems."""
    message = json.dumps({"status": "paused", "reason": reason})
    channel.basic_publish(
        exchange='',
        routing_key=TRADING_PAUSED_QUEUE,
        body=message,
        properties=pika.BasicProperties(delivery_mode=2)
    )
    logger.info("Published trading paused message: %s", message)

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
            # If trading is paused due to daily loss limit, publish a trading_paused message.
            if result.reason and "Daily loss limit reached" in result.reason:
                publish_trading_paused(channel, result.reason)
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
        on_message_callback=lambda ch, method, props, body: on_message(ch, method, props, body, risk_manager)
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

    db_manager = DBManager(conn)
    risk_manager = RiskManager(db_manager)
    start_consuming(risk_manager)

if __name__ == "__main__":
    main()