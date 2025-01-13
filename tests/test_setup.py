# tests/test_setup.py

import pytest
import duckdb
from src.db.duckdb_setup import init_db_and_populate


@pytest.fixture
def test_db_path(tmp_path):
    """Use a temporary path for the DuckDB file so each test is isolated."""
    return str(tmp_path / "test_beverage.db")


def test_init_db_and_populate(test_db_path):
    """Check that product, customer, and sales tables are created and non-empty."""
    # Run the setup on a fresh DB
    init_db_and_populate(
        db_path=test_db_path, num_products=5, num_customers=5, num_sales=10
    )

    # Connect and verify
    conn = duckdb.connect(test_db_path)
    tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]

    # Ensure the tables exist
    assert "product" in tables
    assert "customer" in tables
    assert "sales" in tables

    # Ensure they are not empty
    product_count = conn.execute("SELECT COUNT(*) FROM product").fetchone()[0]
    customer_count = conn.execute("SELECT COUNT(*) FROM customer").fetchone()[0]
    sales_count = conn.execute("SELECT COUNT(*) FROM sales").fetchone()[0]

    assert product_count > 0
    assert customer_count > 0
    assert sales_count > 0

    conn.close()
