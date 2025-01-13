#!/usr/bin/env python3
"""
Pivot the 'supplier_metrics' table to show sales_volume by quarter (Q1, Q2, Q3, Q4).
"""

import duckdb


def pivot_supplier_metrics_by_quarter(db_path: str = "beverage_analysis.db") -> None:
    """
    Creates or replaces the 'supplier_metrics_pivot_by_quarter' table, which pivots sales_volume across Q1, Q2, Q3, and Q4 for each supplier, brand, and family.

    Steps:
    1. Verify 'supplier_metrics' exists in the database.
    2. Pivot the sales_volume for each quarter into separate columns.
    """
    try:
        with duckdb.connect(database=db_path, read_only=False) as conn:
            # 1) Check if 'supplier_metrics' exists
            table_exists_query = """
                SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'supplier_metrics'
            """
            exists = conn.execute(table_exists_query).fetchone()[0]
            if exists == 0:
                print(
                    "[Warning] 'supplier_metrics' table does not exist. Aborting pivot operation."
                )
                return

            # 2) Pivot the data into a new table
            pivot_query = r"""
                CREATE OR REPLACE TABLE supplier_metrics_pivot_by_quarter AS
                SELECT
                    supplier,
                    brand,
                    family,
                    -- Sum sales_volume for each quarter separately
                    SUM(CASE WHEN quarter = 1 THEN sales_volume ELSE 0 END) AS q1_sales_volume,
                    SUM(CASE WHEN quarter = 2 THEN sales_volume ELSE 0 END) AS q2_sales_volume,
                    SUM(CASE WHEN quarter = 3 THEN sales_volume ELSE 0 END) AS q3_sales_volume,
                    SUM(CASE WHEN quarter = 4 THEN sales_volume ELSE 0 END) AS q4_sales_volume
                FROM supplier_metrics
                GROUP BY supplier, brand, family
                ORDER BY supplier, brand, family
            """
            conn.execute(pivot_query)

        print("[Info] 'supplier_metrics_pivot_by_quarter' table created successfully.")
    except Exception as ex:
        print(f"[Error] Failed to pivot supplier metrics: {ex}")


def main():
    pivot_supplier_metrics_by_quarter(db_path="beverage_analysis.db")


if __name__ == "__main__":
    main()
