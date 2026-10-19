"""
Unit Tests for Data Validation, Schema Compliance, and Cleansing.
"""

import pandas as pd
import pytest
from ecommerce_etl.transform import validate_frame, clean_frame


def test_valid_customer_frame_passes():
    """Verifies that a fully populated and valid customer dataset produces no validation errors."""
    frame = pd.DataFrame({
        "customer_id": [1],
        "customer_name": ["A"],
        "email": ["a@example.com"],
        "city": ["Pune"],
        "signup_date": ["2024-01-01"]
    })
    assert validate_frame("customers.csv", frame) == []


def test_duplicate_customer_ids_fail():
    frame = pd.DataFrame({
        "customer_id": [1, 1],
        "customer_name": ["A", "B"],
        "email": ["a@x.com", "b@x.com"],
        "city": ["Pune", "Delhi"],
        "signup_date": ["2024-01-01", "2024-01-02"]
    })
    assert any("duplicate customer_id" in error for error in validate_frame("customers.csv", frame))

def test_missing_columns_fail():
    assert "missing columns" in validate_frame("customers.csv", pd.DataFrame({"customer_id": [1]}))[0]

def test_negative_price_fails():
    frame = pd.DataFrame({
        "product_id": [1],
        "product_name": ["X"],
        "category": ["Home"],
        "unit_price": [-5]
    })
    assert any("negative" in error for error in validate_frame("products.csv", frame))

def test_invalid_quantity_fails():
    frame = pd.DataFrame({
        "order_item_id": [1],
        "order_id": [10],
        "product_id": [5],
        "quantity": [0],
        "unit_price": [15.0]
    })
    assert any("quantity must be a positive number" in error for error in validate_frame("order_items.csv", frame))

def test_clean_frame_deduplicates_and_filters():
    frame = pd.DataFrame({
        "product_id": [1, 1, 2],
        "product_name": [" A ", "A", "B"],
        "category": ["Home", "Home", "Books"],
        "unit_price": [10.0, 10.0, -99.0]
    })
    cleaned = clean_frame("products.csv", frame)
    # Deduplicates product_id 1 and filters out negative price for product_id 2
    assert len(cleaned) == 1
    assert cleaned.iloc[0]["product_id"] == 1
    assert cleaned.iloc[0]["product_name"] == "A"

