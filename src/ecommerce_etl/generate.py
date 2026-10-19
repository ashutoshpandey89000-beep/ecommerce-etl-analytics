"""
Synthetic E-commerce Data Generator Module.

Produces deterministic, schema-compliant CSV datasets representing a normalized
e-commerce retail domain:
  - Customers (demographics and registration history)
  - Products (catalog categories and pricing)
  - Orders (customer transactions and fulfillment status)
  - Order Items (line-item purchases, quantities, and pricing)
  - Payments (transaction timestamps, payment instruments, and settlement states)

Guarantees:
  - Strict temporal consistency: customer signup_date <= order_date <= payment_date.
  - Foreign key referential integrity across all generated entity relationships.
  - Deterministic reproducibility when an identical random seed is supplied.
"""

from pathlib import Path
from datetime import date, timedelta
import logging
import random
from typing import Dict, Union
import pandas as pd

logger = logging.getLogger(__name__)


def generate_data(
    output_dir: Union[str, Path],
    seed: int = 42,
    customer_count: int = 250,
    product_count: int = 40,
    order_count: int = 1200
) -> Dict[str, Path]:
    """
    Generates synthetic e-commerce CSV files and writes them to the specified directory.

    Args:
        output_dir (str or Path): Destination filesystem directory for output CSVs.
        seed (int): Seed value for deterministic pseudo-random number generator.
        customer_count (int): Total unique customer entities to synthesize.
        product_count (int): Total unique catalog products to synthesize.
        order_count (int): Total order transactions to synthesize.

    Returns:
        Dict[str, Path]: Mapping from CSV filename (e.g. 'orders.csv') to absolute Path.
    """
    rng = random.Random(seed)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    cities = ["Pune", "Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Lucknow", "Jaipur"]
    categories = ["Electronics", "Home", "Fashion", "Books", "Sports", "Beauty"]


    # Generate customer signup dates in 2023 (prior to any order dates in 2024-2025)
    customers = [{
        "customer_id": i,
        "customer_name": f"Customer {i:04d}",
        "email": f"customer{i:04d}@example.com",
        "city": rng.choice(cities),
        "signup_date": date(2023, 1, 1) + timedelta(days=rng.randint(0, 364))
    } for i in range(1, customer_count + 1)]

    products = [{
        "product_id": i,
        "product_name": f"Product {i:03d}",
        "category": rng.choice(categories),
        "unit_price": round(rng.uniform(8, 650), 2)
    } for i in range(1, product_count + 1)]

    price_by_id = {p["product_id"]: p["unit_price"] for p in products}
    orders, items, payments = [], [], []

    for oid in range(1, order_count + 1):
        cid = rng.randint(1, customer_count)
        # Orders occur during 2024-2025 (always on or after customer signup in 2023)
        order_date = date(2024, 1, 1) + timedelta(days=rng.randint(0, 729))
        status = rng.choices(["completed", "cancelled", "returned"], weights=[75, 15, 10])[0]

        orders.append({
            "order_id": oid,
            "customer_id": cid,
            "order_date": order_date,
            "order_status": status
        })

        total = 0.0
        item_count = rng.randint(1, 4)
        for _ in range(item_count):
            pid = rng.randint(1, product_count)
            qty = rng.randint(1, 5)
            price = price_by_id[pid]
            items.append({
                "order_item_id": len(items) + 1,
                "order_id": oid,
                "product_id": pid,
                "quantity": qty,
                "unit_price": price
            })
            total += qty * price

        total = round(total, 2)
        if status == "completed":
            pay_status = "paid"
        elif status == "cancelled":
            pay_status = rng.choice(["failed", "cancelled", "refunded"])
        else:
            pay_status = "refunded"

        payments.append({
            "payment_id": oid,
            "order_id": oid,
            "payment_date": order_date,
            "payment_method": rng.choice(["card", "upi", "net_banking", "wallet"]),
            "payment_status": pay_status,
            "amount": total
        })

    datasets = {
        "customers.csv": customers,
        "products.csv": products,
        "orders.csv": orders,
        "order_items.csv": items,
        "payments.csv": payments
    }

    paths = {}
    for filename, rows in datasets.items():
        path = out / filename
        pd.DataFrame(rows).to_csv(path, index=False)
        paths[filename] = path
        logger.info(f"Generated {len(rows)} records into {path}")

    return paths

