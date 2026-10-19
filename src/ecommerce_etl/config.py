"""
Configuration module for the E-commerce ETL & Sales Analytics Pipeline.

Handles environment variable resolution with sensible local defaults, supporting
both direct host execution (PowerShell) and containerized Docker networking.
"""

import os
from dotenv import load_dotenv

# Load environment configuration from .env if present
load_dotenv()


def database_url() -> str:
    """
    Constructs and returns the PostgreSQL SQLAlchemy connection URI.

    Environment variables:
        POSTGRES_HOST: Database hostname ('localhost' on host, 'postgres' inside Docker network).
        POSTGRES_PORT: Database listening port (default: 5432).
        POSTGRES_DB: Database catalog name (default: 'ecommerce').
        POSTGRES_USER: Authenticated database user (default: 'ecommerce').
        POSTGRES_PASSWORD: User password credentials.

    Returns:
        str: Fully qualified SQLAlchemy connection string using psycopg2 dialect.
    """
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "ecommerce")
    user = os.getenv("POSTGRES_USER", "ecommerce")
    password = os.getenv("POSTGRES_PASSWORD", "change_me_local_only")

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

