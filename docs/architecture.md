# Architecture

1. Generate deterministic CSV datasets for customers, products, orders, order items and payments.
2. Validate required columns, primary-key nulls/duplicates, numeric ranges and foreign-key references.
3. Parse date columns and standardize inputs.
4. Load relational tables in PostgreSQL inside a transaction.
5. Publish SQL views for monthly sales, product revenue, customer segments and payment summaries.
6. Orchestrate generation and ETL using Airflow.
7. Run unit tests for validation behavior.

Synthetic data keeps the project reproducible and avoids exposing personal customer information. It demonstrates engineering workflow rather than real business outcomes.
