"""
Data Transformation, Validation, and Quality Assurance Module.

Implements data quality contracts across all incoming e-commerce entities:
  - Schema conformity (ensuring all mandatory fields are present)
  - Entity integrity (primary key null & duplicate detection)
  - Domain constraints (non-negative currency amounts, positive item quantities)
  - Referential integrity (validating foreign keys across parent/child tables)
  - Data cleansing & normalization (whitespace trimming, data type coercion, date parsing)
"""

from pathlib import Path
import logging
from datetime import date
from typing import Dict, List, Set, Union
import pandas as pd

logger = logging.getLogger(__name__)

# Required column schemas per entity table
REQUIRED: Dict[str, Set[str]] = {
    "customers.csv": {"customer_id", "customer_name", "email", "city", "signup_date"},
    "products.csv": {"product_id", "product_name", "category", "unit_price"},
    "orders.csv": {"order_id", "customer_id", "order_date", "order_status"},
    "order_items.csv": {"order_item_id", "order_id", "product_id", "quantity", "unit_price"},
    "payments.csv": {"payment_id", "order_id", "payment_date", "payment_method", "payment_status", "amount"},
}

# Primary key identifier per entity table
ID_COL: Dict[str, str] = {
    "customers.csv": "customer_id",
    "products.csv": "product_id",
    "orders.csv": "order_id",
    "order_items.csv": "order_item_id",
    "payments.csv": "payment_id"
}


def validate_frame(filename: str, frame: pd.DataFrame) -> List[str]:
    """
    Validates a single DataFrame against schema, uniqueness, numeric ranges, and completeness rules.

    Args:
        filename (str): Name of the source CSV file (e.g., 'customers.csv').
        frame (pd.DataFrame): Ingested DataFrame to validate.

    Returns:
        List[str]: List of human-readable data quality error descriptions. Returns empty list if valid.
    """
    errors = []
    missing = REQUIRED[filename] - set(frame.columns)
    if missing:
        return [f"{filename}: missing columns {sorted(missing)}"]

    id_col = ID_COL[filename]

    # Primary key null checks
    if frame[id_col].isna().any():
        errors.append(f"{filename}: null {id_col}")

    # Primary key uniqueness checks
    if frame[id_col].duplicated().any():
        errors.append(f"{filename}: duplicate {id_col}")

    # Null checks across all required columns
    for col in REQUIRED[filename]:
        if col != id_col and frame[col].isna().any():
            errors.append(f"{filename}: null values found in required column '{col}'")

    # Numeric range checks for prices and payment amounts
    amount_col = "amount" if filename == "payments.csv" else "unit_price"
    if amount_col in frame:
        numeric_series = pd.to_numeric(frame[amount_col], errors="coerce")
        if numeric_series.isna().any() or (numeric_series < 0).any():
            errors.append(f"{filename}: invalid or negative {amount_col}")

    # Positive integer quantity checks for order items
    if filename == "order_items.csv":
        qty_series = pd.to_numeric(frame["quantity"], errors="coerce")
        if qty_series.isna().any() or (qty_series <= 0).any():
            errors.append(f"{filename}: quantity must be a positive number")

    return errors


def clean_frame(filename: str, frame: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans a dataset by dropping rows with null primary keys, removing duplicates,
    stripping string whitespace, filtering out invalid numeric values, and casting types.

    Args:
        filename (str): Name of the source CSV file (e.g. 'products.csv').
        frame (pd.DataFrame): Raw ingested DataFrame.

    Returns:
        pd.DataFrame: Cleaned, deduplicated, and strictly typed DataFrame.
    """
    df = frame.copy()
    id_col = ID_COL[filename]

    # Drop rows where primary key is missing
    df = df.dropna(subset=[id_col])

    # Deduplicate on primary key, retaining the first occurrence
    df = df.drop_duplicates(subset=[id_col], keep="first")

    # Clean string columns: strip whitespace
    for col in df.select_dtypes(include=["object", "string"]).columns:
        if not col.endswith("_date"):
            df[col] = df[col].astype(str).str.strip()

    # Clean numeric fields and cast entity IDs
    if filename == "products.csv":
        df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
        df = df[df["unit_price"].notna() & (df["unit_price"] >= 0)]
        df["unit_price"] = df["unit_price"].round(2)
        df["product_id"] = df["product_id"].astype(int)

    elif filename == "order_items.csv":
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
        df = df[df["quantity"].notna() & (df["quantity"] > 0)]
        df = df[df["unit_price"].notna() & (df["unit_price"] >= 0)]
        df["quantity"] = df["quantity"].astype(int)
        df["unit_price"] = df["unit_price"].round(2)
        df["order_item_id"] = df["order_item_id"].astype(int)
        df["order_id"] = df["order_id"].astype(int)
        df["product_id"] = df["product_id"].astype(int)

    elif filename == "payments.csv":
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        df = df[df["amount"].notna() & (df["amount"] >= 0)]
        df["amount"] = df["amount"].round(2)
        df["payment_id"] = df["payment_id"].astype(int)
        df["order_id"] = df["order_id"].astype(int)

    elif filename == "customers.csv":
        df["customer_id"] = df["customer_id"].astype(int)

    elif filename == "orders.csv":
        df["order_id"] = df["order_id"].astype(int)
        df["customer_id"] = df["customer_id"].astype(int)

    # Standardize date columns to datetime.date objects
    for col in df.columns:
        if col.endswith("_date"):
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            df = df[df[col].notna()]

    return df


def load_and_validate(
    raw_dir: Union[str, Path],
    clean_data: bool = False
) -> Dict[str, pd.DataFrame]:
    """
    Ingests raw CSV files, optionally cleans dirty inputs, verifies schema and data
    integrity, validates foreign key relationships, and returns typed DataFrames.

    Args:
        raw_dir (str or Path): Filesystem path to the folder containing raw CSV files.
        clean_data (bool): If True, applies clean_frame() preprocessing prior to validation.

    Returns:
        Dict[str, pd.DataFrame]: Dictionary mapping filename to validated DataFrame.

    Raises:
        ValueError: If mandatory files are missing, schema constraints fail, or
                    foreign key referential integrity checks fail.
    """
    raw = Path(raw_dir)
    frames: Dict[str, pd.DataFrame] = {}
    errors: List[str] = []

    for filename in REQUIRED:
        path = raw / filename
        if not path.exists():
            errors.append(f"Missing input file: {path}")
            continue
        frame = pd.read_csv(path)
        if clean_data:
            frame = clean_frame(filename, frame)
        val_errors = validate_frame(filename, frame)
        errors.extend(val_errors)
        frames[filename] = frame

    if errors:
        error_msg = "Data quality checks failed:\n- " + "\n- ".join(errors)
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Enforce relational foreign key integrity across domain boundaries
    customers = set(frames["customers.csv"]["customer_id"])
    products = set(frames["products.csv"]["product_id"])
    orders = set(frames["orders.csv"]["order_id"])

    if not set(frames["orders.csv"]["customer_id"]).issubset(customers):
        raise ValueError("orders.csv contains unknown customer_id references")
    if not set(frames["order_items.csv"]["order_id"]).issubset(orders):
        raise ValueError("order_items.csv contains unknown order_id references")
    if not set(frames["order_items.csv"]["product_id"]).issubset(products):
        raise ValueError("order_items.csv contains unknown product_id references")
    if not set(frames["payments.csv"]["order_id"]).issubset(orders):
        raise ValueError("payments.csv contains unknown order_id references")

    # Ensure date columns are formatted as datetime.date objects for SQL insertion
    for filename, frame in frames.items():
        for col in frame.columns:
            if col.endswith("_date"):
                frame[col] = pd.to_datetime(frame[col], errors="raise").dt.date
        logger.info(f"Loaded and validated {filename} ({len(frame)} rows)")

    return frames


