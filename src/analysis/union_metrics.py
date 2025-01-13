#!/usr/bin/env python3
"""
Create a 'union_metrics' table by:
1. Union-ing supplier_metrics and customer_supplier_metrics via 'UNION BY NAME'.
2. Applying another round of GROUPING SETS to produce aggregated rows.
3. Including 'quarter' in the grouping sets, with 0 as a placeholder for "ALL QUARTERS".
"""

import duckdb


def create_union_metrics(db_path: str = "beverage_analysis.db") -> None:
    """
    Creates or replaces 'union_metrics' by:
      - Unioning rows from supplier_metrics & customer_supplier_metrics
      - Handling columns that don't exist in one table via NULL placeholders
      - Grouping by quarter as well, using COALESCE(quarter, 0)
      - Summing sales_volume, price, and cost
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
            #    Then group by (supplier, brand, family, customer_type, quarter).
            #    We'll treat NULL quarters as 0 => means "ALL QUARTERS".
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
                SELECT DISTINCT  -- Removes any truly identical rows
                    COALESCE(supplier, 'ALL SUPPLIERS')       AS supplier,
                    COALESCE(brand, 'ALL BRANDS')             AS brand,
                    COALESCE(family, 'ALL FAMILIES')          AS family,
                    COALESCE(customer_type, 'ALL CUST TYPES') AS customer_type,
                    COALESCE(quarter, 0)                      AS quarter,
                    SUM(sales_volume) AS sales_volume,
                    SUM(price)        AS price,
                    SUM(cost)         AS cost
                FROM union_cte
                GROUP BY GROUPING SETS (
                    (supplier, brand, family, customer_type, quarter),
                    (supplier, brand, family, quarter),
                    (supplier, brand, quarter),
                    (supplier, quarter),
                    (quarter)
                );
            """

            conn.execute(union_grouping_query)

        print(
            "[Info] 'union_metrics' table created successfully (with grouping sets, including quarter)."
        )

    except Exception as ex:
        print(f"[Error] Failed to create 'union_metrics' table: {ex}")


def main():
    create_union_metrics("beverage_analysis.db")


if __name__ == "__main__":
    main()
