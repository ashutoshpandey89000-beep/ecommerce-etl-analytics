"""
Command-Line Interface (CLI) Entrypoint for E-commerce ETL Operations.

Provides subcommands to:
  1. 'generate': Synthesize deterministic CSV datasets.
  2. 'run': Clean, validate, and load raw CSV datasets into PostgreSQL.
"""

import argparse
import json
import logging
import sys
from .generate import generate_data
from .load import run_pipeline


def setup_logging() -> None:
    """Configures structured console logging for pipeline execution."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)]
    )


def main() -> None:
    """Parses command-line arguments and dispatches pipeline operations."""

    setup_logging()
    parser = argparse.ArgumentParser(description="E-commerce ETL pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="Generate synthetic e-commerce CSV datasets")
    gen.add_argument("--output-dir", default="data/raw", help="Target directory for generated CSVs")
    gen.add_argument("--customers", type=int, default=250, help="Number of customers to generate")
    gen.add_argument("--products", type=int, default=40, help="Number of products to generate")
    gen.add_argument("--orders", type=int, default=1200, help="Number of orders to generate")

    run = sub.add_parser("run", help="Run validation, transformation, and PostgreSQL loading")
    run.add_argument("--input-dir", default="data/raw", help="Directory containing raw CSV files")
    run.add_argument("--clean", action="store_true", help="Automatically clean malformed records before loading")

    args = parser.parse_args()
    if args.command == "generate":
        result = generate_data(
            args.output_dir,
            customer_count=args.customers,
            product_count=args.products,
            order_count=args.orders
        )
        print(json.dumps({k: str(v) for k, v in result.items()}, indent=2))
    else:
        loaded = run_pipeline(args.input_dir, clean_data=args.clean)
        print("Pipeline completed. Rows loaded:")
        print(json.dumps(loaded, indent=2))

if __name__ == "__main__":
    main()

