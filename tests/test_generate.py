"""
Unit Tests for Synthetic Data Generation and Temporal Integrity.
"""

from pathlib import Path
import pandas as pd
from ecommerce_etl.generate import generate_data


def test_generate_data_creates_all_files(tmp_path):
    """Verifies all 5 mandatory CSV files are synthesized with non-empty rows."""

    output_dir = tmp_path / "raw"
    files = generate_data(output_dir, customer_count=20, product_count=5, order_count=30)
    
    expected_files = ["customers.csv", "products.csv", "orders.csv", "order_items.csv", "payments.csv"]
    for fname in expected_files:
        assert fname in files
        assert Path(files[fname]).exists()
        df = pd.read_csv(files[fname])
        assert len(df) > 0

def test_generate_data_is_deterministic(tmp_path):
    dir1 = tmp_path / "run1"
    dir2 = tmp_path / "run2"
    generate_data(dir1, seed=123, customer_count=10, product_count=5, order_count=15)
    generate_data(dir2, seed=123, customer_count=10, product_count=5, order_count=15)

    for fname in ["customers.csv", "products.csv", "orders.csv", "order_items.csv", "payments.csv"]:
        df1 = pd.read_csv(dir1 / fname)
        df2 = pd.read_csv(dir2 / fname)
        pd.testing.assert_frame_equal(df1, df2)

def test_signup_date_precedes_or_matches_order_date(tmp_path):
    out = tmp_path / "data"
    generate_data(out, seed=42, customer_count=30, product_count=5, order_count=50)
    
    customers = pd.read_csv(out / "customers.csv")
    orders = pd.read_csv(out / "orders.csv")

    merged = orders.merge(customers, on="customer_id")
    # All signup dates are in 2023, order dates in 2024-2025
    assert (pd.to_datetime(merged["signup_date"]) <= pd.to_datetime(merged["order_date"])).all()
