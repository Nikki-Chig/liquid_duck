#!/usr/bin/env python3
"""
Create the 'supplier_metrics' table by aggregating data
from the 'product' and 'sales' tables using DuckDB's GROUPING SETS.

Tables Involved:
- product (supplier, brand, family, etc.)
- sales   (sales_volume, price, cost, sold_on_date)

Resulting Table:
- supplier_metrics
"""

import duckdb


def create_supplier_metrics(db_path: str = "beverage_analysis.db") -> None:
    """
    Creates the supplier_metrics table, which aggregates sales information at different hierarchy levels (supplier, brand, family, quarter).
    :param db_path: Path to the DuckDB database file.
    """
    query = r"""
        CREATE OR REPLACE TABLE supplier_metrics AS
        SELECT
            supplier,
            brand,
            family,
            CAST(((sold_on_date % 100) - 1) / 3 + 1 AS INTEGER) AS quarter,
            SUM(sales_volume) AS sales_volume,
            SUM(price)        AS price,
            SUM(cost)         AS cost
        FROM (
            SELECT
                p.supplier,
                p.brand,
                p.family,
                s.sold_on_date,
                s.sales_volume,
                s.price,
                s.cost
            FROM product p
            JOIN sales s
                ON p.product_id = s.product_id
        )
        GROUP BY GROUPING SETS (
            (supplier, brand, family, quarter),
            (supplier, brand, quarter),
            (supplier, quarter),
            (quarter)
        );
    """

    try:
        with duckdb.connect(database=db_path, read_only=False) as conn:
            conn.execute(query)
        print("[Info] 'supplier_metrics' table created successfully.")
    except Exception as ex:
        print(f"[Error] Failed to create 'supplier_metrics' table: {ex}")


def main():
    create_supplier_metrics(db_path="beverage_analysis.db")


if __name__ == "__main__":
    main()
