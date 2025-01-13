#!/usr/bin/env python3
"""
Creates or replaces the 'customer_supplier_metrics' table by joining
'product', 'sales', and 'customer' tables, grouping by (supplier, customer_type).
"""

import duckdb


def create_customer_supplier_metrics(db_path: str = "beverage_analysis.db") -> None:
    """
    Creates or replaces a table named 'customer_supplier_metrics'

    Aggregates data from product, sales, and customer tables:
      SUM(sales_volume), SUM(price), and SUM(cost),
      grouped by (supplier, customer_type).
    """
    try:
        with duckdb.connect(database=db_path, read_only=False) as conn:
            # Optional: check if the required tables exist
            required_tables = {"product", "sales", "customer"}
            existing_tables = set(
                row[0] for row in conn.execute("SHOW TABLES").fetchall()
            )

            missing = required_tables - existing_tables
            if missing:
                print(f"[Warning] Missing table(s): {missing}. Aborting operation.")
                return

            # Create or replace the new aggregation table
            query = r"""
                CREATE OR REPLACE TABLE customer_supplier_metrics AS
                SELECT
                    p.supplier       AS supplier,
                    c.customer_type  AS customer_type,
                    SUM(s.sales_volume) AS sales_volume,
                    SUM(s.price)        AS price,
                    SUM(s.cost)         AS cost
                FROM product p
                JOIN sales s
                    ON p.product_id = s.product_id
                JOIN customer c
                    ON c.customer_id = s.customer_id
                GROUP BY p.supplier, c.customer_type
            """

            conn.execute(query)
        print("[Info] 'customer_supplier_metrics' table created successfully.")
    except Exception as ex:
        print(f"[Error] Failed to create 'customer_supplier_metrics' table: {ex}")


def main():
    create_customer_supplier_metrics("beverage_analysis.db")


if __name__ == "__main__":
    main()
