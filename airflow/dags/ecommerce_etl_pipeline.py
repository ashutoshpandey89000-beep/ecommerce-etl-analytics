"""
Apache Airflow DAG: E-commerce ETL & Sales Analytics Pipeline.

Orchestrates the sequential lifecycle:
  1. 'generate_sample_data': Produces raw synthetic CSV files with deterministic integrity.
  2. 'validate_transform_load': Cleans, validates, and atomically loads data into PostgreSQL,
     then deploys SQL analytics reporting views.

Execution pattern:
  generate_sample_data >> validate_transform_load
"""

from datetime import datetime
import logging
import sys
from typing import Any, Dict
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

PROJECT = "/opt/airflow/project"
if f"{PROJECT}/src" not in sys.path:
    sys.path.insert(0, f"{PROJECT}/src")

from ecommerce_etl.generate import generate_data
from ecommerce_etl.load import run_pipeline

RAW_DIR = f"{PROJECT}/data/raw"


def generate_task(**context):
    logger.info(f"Starting synthetic data generation into {RAW_DIR}")
    paths = generate_data(RAW_DIR)
    summary = {k: str(v) for k, v in paths.items()}
    logger.info(f"Data generation complete: {summary}")
    return summary

def etl_task(**context):
    logger.info(f"Starting validation, transformation, and load from {RAW_DIR}")
    result = run_pipeline(RAW_DIR)
    logger.info(f"ETL pipeline finished successfully. Loaded rows: {result}")
    return result

with DAG(
    dag_id="ecommerce_etl_pipeline",
    description="Generate, validate, transform, and load e-commerce sample data into PostgreSQL",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["portfolio", "etl", "postgres", "analytics"],
) as dag:
    generate = PythonOperator(
        task_id="generate_sample_data",
        python_callable=generate_task,
    )
    etl = PythonOperator(
        task_id="validate_transform_load",
        python_callable=etl_task,
    )
    generate >> etl

