"""
Database Loading and Analytics Schema Publishing Module.

Manages relational database lifecycle, DDL schema definitions, idempotent data
ingestion, and analytical reporting views:
  - Ensures schema `analytics` and staging tables exist.
  - Implements transactional DDL & DML (`engine.begin()`) with ACID guarantees.
  - Guarantees idempotent pipeline re-runs via `TRUNCATE ... CASCADE`.
  - Publishes aggregated views for executive business intelligence.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from .config import database_url

logger = logging.getLogger(__name__)

# Structured DDL statements for relational schema and analytical reporting views
DDL: List[str] = [
'''CREATE SCHEMA IF NOT EXISTS analytics''',

'''CREATE TABLE IF NOT EXISTS raw_customers (
 customer_id INTEGER PRIMARY KEY, customer_name TEXT NOT NULL, email TEXT NOT NULL,
 city TEXT NOT NULL, signup_date DATE NOT NULL)''',
'''CREATE TABLE IF NOT EXISTS raw_products (
 product_id INTEGER PRIMARY KEY, product_name TEXT NOT NULL, category TEXT NOT NULL,
 unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0))''',
'''CREATE TABLE IF NOT EXISTS raw_orders (
 order_id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL REFERENCES raw_customers(customer_id),
 order_date DATE NOT NULL, order_status TEXT NOT NULL)''',
'''CREATE TABLE IF NOT EXISTS raw_order_items (
 order_item_id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL REFERENCES raw_orders(order_id),
 product_id INTEGER NOT NULL REFERENCES raw_products(product_id), quantity INTEGER NOT NULL CHECK (quantity > 0),
 unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0))''',
'''CREATE TABLE IF NOT EXISTS raw_payments (
 payment_id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL REFERENCES raw_orders(order_id),
 payment_date DATE NOT NULL, payment_method TEXT NOT NULL, payment_status TEXT NOT NULL,
 amount NUMERIC(12,2) NOT NULL CHECK (amount >= 0))''',
'''CREATE OR REPLACE VIEW analytics.monthly_sales AS
 SELECT date_trunc('month', o.order_date)::date AS month,
 COUNT(DISTINCT o.order_id) AS order_count,
 ROUND(SUM(i.quantity * i.unit_price), 2) AS gross_sales
 FROM raw_orders o JOIN raw_order_items i ON i.order_id=o.order_id
 WHERE o.order_status='completed' GROUP BY 1''',
'''CREATE OR REPLACE VIEW analytics.top_products AS
 SELECT p.product_id, p.product_name, p.category, SUM(i.quantity) AS units_sold,
 ROUND(SUM(i.quantity*i.unit_price),2) AS revenue
 FROM raw_products p JOIN raw_order_items i ON i.product_id=p.product_id
 JOIN raw_orders o ON o.order_id=i.order_id WHERE o.order_status='completed'
 GROUP BY p.product_id,p.product_name,p.category''',
'''CREATE OR REPLACE VIEW analytics.customer_segments AS
 WITH metrics AS (
 SELECT c.customer_id,c.customer_name,c.city,
 COUNT(DISTINCT o.order_id) FILTER (WHERE o.order_status='completed') AS completed_orders,
 COALESCE(SUM(i.quantity*i.unit_price) FILTER (WHERE o.order_status='completed'),0) AS lifetime_value,
 MAX(o.order_date) FILTER (WHERE o.order_status='completed') AS last_order_date
 FROM raw_customers c LEFT JOIN raw_orders o ON o.customer_id=c.customer_id
 LEFT JOIN raw_order_items i ON i.order_id=o.order_id GROUP BY c.customer_id,c.customer_name,c.city)
 SELECT *, CASE WHEN completed_orders>=8 THEN 'loyal'
 WHEN completed_orders>=3 THEN 'repeat' WHEN completed_orders>=1 THEN 'new_or_active'
 ELSE 'no_completed_orders' END AS segment FROM metrics''',
'''CREATE OR REPLACE VIEW analytics.payment_summary AS
 SELECT payment_method,payment_status,COUNT(*) AS payment_count,ROUND(SUM(amount),2) AS total_amount
 FROM raw_payments GROUP BY payment_method,payment_status'''
]

ORDER: List[Tuple[str, str]] = [
    ("customers.csv", "raw_customers"),
    ("products.csv", "raw_products"),
    ("orders.csv", "raw_orders"),
    ("order_items.csv", "raw_order_items"),
    ("payments.csv", "raw_payments")
]


def run_pipeline(
    raw_dir: Union[str, Path],
    engine: Optional[Engine] = None,
    clean_data: bool = False
) -> Dict[str, int]:
    """
    Executes the end-to-end relational ETL workflow:
      1. Validates and loads source CSV datasets into memory.
      2. Ensures schema, tables, and analytics views are deployed.
      3. Idempotently truncates prior data to prevent duplication.
      4. Inserts transformed records in dependency order inside a single transaction.

    Args:
        raw_dir (str or Path): Folder location containing validated input CSVs.
        engine (Optional[Engine]): Optional pre-configured SQLAlchemy Engine instance.
        clean_data (bool): Whether to clean and filter malformed inputs automatically.

    Returns:
        Dict[str, int]: Dictionary mapping CSV filename to the count of loaded records.

    Raises:
        Exception: Re-raises any underlying database or validation error after rollback.
    """
    from .transform import load_and_validate
    logger.info(f"Starting ETL pipeline for directory: {raw_dir}")
    frames = load_and_validate(raw_dir, clean_data=clean_data)

    owns_engine = engine is None
    engine = engine or create_engine(database_url(), pool_pre_ping=True)

    try:
        # Execute DDL, truncation, and data ingestion within an atomic transaction
        with engine.begin() as conn:
            logger.info("Ensuring schema, tables, and analytics views exist")
            for statement in DDL:
                conn.execute(text(statement))

            logger.info("Truncating raw tables for deterministic, idempotent load")
            conn.execute(text("TRUNCATE raw_payments, raw_order_items, raw_orders, raw_products, raw_customers CASCADE"))

            for filename, table in ORDER:
                df = frames[filename]
                df.to_sql(table, conn, if_exists="append", index=False, method="multi", chunksize=1000)
                logger.info(f"Loaded {len(df)} rows into table '{table}'")

        loaded_summary = {filename: len(frame) for filename, frame in frames.items()}
        logger.info(f"Pipeline executed successfully. Loaded rows: {loaded_summary}")
        return loaded_summary
    except Exception as exc:
        logger.error(f"Pipeline execution failed: {exc}", exc_info=True)
        raise
    finally:
        if owns_engine:
            engine.dispose()


def get_table_counts(engine: Optional[Engine] = None) -> Dict[str, int]:
    """
    Retrieves live row counts for all raw relational tables in PostgreSQL.

    Args:
        engine (Optional[Engine]): Optional pre-configured SQLAlchemy Engine.

    Returns:
        Dict[str, int]: Mapping from table name to current record count.
    """
    owns_engine = engine is None
    engine = engine or create_engine(database_url(), pool_pre_ping=True)
    try:
        tables = ["raw_customers", "raw_products", "raw_orders", "raw_order_items", "raw_payments"]
        counts: Dict[str, int] = {}
        with engine.connect() as conn:
            for table in tables:
                res = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                counts[table] = int(res) if res is not None else 0
        return counts
    finally:
        if owns_engine:
            engine.dispose()


