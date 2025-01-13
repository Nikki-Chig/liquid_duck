#!/usr/bin/env python3
"""
Create a 'union_metrics' table by:
1. Union-ing supplier_metrics and customer_supplier_metrics via 'UNION BY NAME'.
2. Applying another round of GROUPING SETS to produce aggregated rows.
3. Removing duplicates (if needed) via DISTINCT
"""

import duckdb


def create_union_metrics(db_path: str = "beverage_analysis.db") -> None:
    """
    Creates or replaces 'union_metrics' by:
      - Unioning rows from supplier_metrics & customer_supplier_metrics
      - Handling columns that don't exist in one table via NULL placeholders
      - Applying grouping sets again
      - Removing duplicates with DISTINCT if desired
    """
    try:
        with duckdb.connect(database=db_path, read_only=False) as conn:

            # 1) Ensure the source tables exist
            required_tables = {"supplier_metrics", "customer_supplier_metrics"}
            existing_tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
            missing = required_tables - existing_tables
            if missing:
                print(
                    f"[Warning] Missing table(s): {missing}. Aborting union operation."
                )
                return

            # 2) Union the two tables by matching column names.
            union_grouping_query = r"""
                CREATE OR REPLACE TABLE union_metrics AS
                WITH union_cte AS (
                    SELECT
                        supplier,
                        brand,
                        family,
                        quarter,
                        NULL AS customer_type,
                        sales_volume,
                        price,
                        cost
                    FROM supplier_metrics

                    UNION ALL BY NAME

                    SELECT
                        supplier,
                        NULL AS brand,
                        NULL AS family,
                        NULL AS quarter,
                        customer_type,
                        sales_volume,
                        price,
                        cost
                    FROM customer_supplier_metrics
                )
                SELECT DISTINCT  -- Removes any exact duplicates. You can omit if you want duplicates.
                    COALESCE(supplier, 'ALL SUPPLIERS')       AS supplier,
                    COALESCE(brand, 'ALL BRANDS')             AS brand,
                    COALESCE(family, 'ALL FAMILIES')          AS family,
                    COALESCE(customer_type, 'ALL CUST TYPES') AS customer_type,
                    -- Summation across rows that share the same grouping
                    SUM(sales_volume) AS sales_volume,
                    SUM(price)        AS price,
                    SUM(cost)         AS cost
                FROM union_cte
                GROUP BY GROUPING SETS (
                    (supplier, brand, family, customer_type),
                    (supplier, brand, customer_type),
                    (supplier, customer_type),
                    (customer_type)
                );
            """

            conn.execute(union_grouping_query)

        print("[Info] 'union_metrics' table created successfully (with grouping sets).")

    except Exception as ex:
        print(f"[Error] Failed to create 'union_metrics' table: {ex}")


def main():
    create_union_metrics("beverage_analysis.db")


if __name__ == "__main__":
    main()
