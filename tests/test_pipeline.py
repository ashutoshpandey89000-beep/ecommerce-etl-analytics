"""
Integration and End-to-End Pipeline Verification Tests.

Validates foreign key referential integrity failures, missing source files,
transactional PostgreSQL ingestion, repeatable idempotent execution, and analytical views.
"""

from pathlib import Path
import pytest
from sqlalchemy import create_engine, text
from ecommerce_etl.generate import generate_data
from ecommerce_etl.transform import load_and_validate
from ecommerce_etl.load import run_pipeline, get_table_counts
from ecommerce_etl.config import database_url


def test_referential_integrity_unknown_customer(tmp_path):
    """Verifies that orders referencing a non-existent customer_id raise an explicit ValueError."""

    out = tmp_path / "raw"
    generate_data(out, customer_count=5, product_count=2, order_count=5)
    
    orders_path = out / "orders.csv"
    import pandas as pd
    orders = pd.read_csv(orders_path)
    # Inject unknown customer ID
    orders.loc[0, "customer_id"] = 99999
    orders.to_csv(orders_path, index=False)

    with pytest.raises(ValueError, match="unknown customer_id references"):
        load_and_validate(out)

def test_missing_csv_file_raises_error(tmp_path):
    out = tmp_path / "incomplete"
    out.mkdir()
    with pytest.raises(ValueError, match="Missing input file"):
        load_and_validate(out)

def test_pipeline_database_load_and_views(tmp_path):
    out = tmp_path / "raw_db_test"
    generate_data(out, customer_count=10, product_count=5, order_count=20)
    
    # Run pipeline
    summary = run_pipeline(out)
    assert summary["customers.csv"] == 10
    assert summary["products.csv"] == 5
    assert summary["orders.csv"] == 20

    # Verify repeatable execution: running again should not duplicate rows
    summary2 = run_pipeline(out)
    assert summary2 == summary

    # Verify database counts
    counts = get_table_counts()
    assert counts["raw_customers"] == 10
    assert counts["raw_products"] == 5
    assert counts["raw_orders"] == 20

    # Verify SQL analytics views return valid results
    engine = create_engine(database_url(), pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            monthly_sales = conn.execute(text("SELECT * FROM analytics.monthly_sales")).fetchall()
            assert len(monthly_sales) > 0

            top_products = conn.execute(text("SELECT * FROM analytics.top_products")).fetchall()
            assert len(top_products) > 0

            customer_segments = conn.execute(text("SELECT * FROM analytics.customer_segments")).fetchall()
            assert len(customer_segments) == 10

            payments = conn.execute(text("SELECT * FROM analytics.payment_summary")).fetchall()
            assert len(payments) > 0
    finally:
        engine.dispose()
