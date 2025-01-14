import duckdb
import pandas as pd
import os


def export_db_tables(
    db_path: str = "beverage_analysis.db",
    output_type: str = "excel",
    output_folder: str = "data",
):
    """
    Reads the 'product', 'customer', and 'sales' tables from a DuckDB database
    and exports them as either CSV or Excel files based on the given output_type.

    Default output_type is 'excel'. Users can modify the output_type to 'csv' as needed.

    Args:
    - db_path: Path to the DuckDB database file.
    - output_type: Either "excel" or "csv". Defaults to "excel".
    - output_folder: The folder to store generated files.
    """
    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # File paths
    excel_file = os.path.join(output_folder, "beverage_analysis.xlsx")

    # Connect to DuckDB
    conn = duckdb.connect(database=db_path, read_only=True)

    # Fetch each table into a DataFrame
    tables = {
        "Product": conn.execute("SELECT * FROM product").fetchdf(),
        "Customer": conn.execute("SELECT * FROM customer").fetchdf(),
        "Sales": conn.execute("SELECT * FROM sales").fetchdf(),
        "Supplier Metrics": conn.execute("SELECT * FROM supplier_metrics").fetchdf(),
        "Supplier Metrics Pivot": conn.execute(
            "SELECT * FROM supplier_metrics_pivot_by_quarter"
        ).fetchdf(),
        "Customer Supplier Metrics": conn.execute(
            "SELECT * FROM customer_supplier_metrics"
        ).fetchdf(),
        "Union Metrics": conn.execute("SELECT * FROM union_metrics").fetchdf(),
    }

    # Close the database connection
    conn.close()

    if output_type.lower() == "excel":
        # Write to Excel with multiple sheets
        with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
            for sheet_name, df in tables.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"[Info] Exported tables to Excel: {excel_file}")

    elif output_type.lower() == "csv":
        # Write each table to a separate CSV
        for sheet_name, df in tables.items():
            csv_file = os.path.join(
                output_folder, f"{sheet_name.replace(' ', '_').lower()}.csv"
            )
            df.to_csv(csv_file, index=False)
            print(f"[Info] Exported '{sheet_name}' table to CSV: {csv_file}")

    else:
        print("[Error] Invalid output type. Please specify 'excel' or 'csv'.")


if __name__ == "__main__":
    # Default to Excel file generation
    export_db_tables()
    # for CSV file generation, use:
    # export_db_tables(output_type="csv")
