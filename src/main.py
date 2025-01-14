#!/usr/bin/env python3
"""
Orchestrator script to create and populate all tables in sequence.
"""

from db import duckdb_setup
from analysis import (
    supplier_metrics,
    supplier_metrics_pivot,
    customer_supplier_metrics,
    union_metrics,
)
from utils import csv_excel_generation


def main():
    """Run all table creation scripts in order."""
    # 1. Create the base tables (product, customer, sales) and populate data
    duckdb_setup.init_db_and_populate()

    # 2. Create derived tables
    supplier_metrics.create_supplier_metrics()
    supplier_metrics_pivot.pivot_supplier_metrics_by_quarter()
    customer_supplier_metrics.create_customer_supplier_metrics()
    union_metrics.create_union_metrics()
    print("[Info] All tables created successfully.")

    # 3. Generate excel reports
    csv_excel_generation.export_db_tables()
    print("[Info] Reports generated successfully in /data folder.")


if __name__ == "__main__":
    main()
