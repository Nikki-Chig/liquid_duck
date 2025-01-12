#!/usr/bin/env python3
"""
Refined DuckDB Setup Script

Creates three DuckDB tables (`product`, `customer`, `sales`) and populates them
with fake data using Faker. Implements PEP 8 standards, pydantic for validation,
and error handling.
"""

import random
from typing import List

import duckdb
from faker import Faker
from pydantic import BaseModel, ValidationError

# ----------------------------------------
# Pydantic Models
# ----------------------------------------


class ProductRecord(BaseModel):
    """Represents a single product row."""

    product_id: int
    supplier: str
    brand: str
    family: str
    color: str
    name: str


class CustomerRecord(BaseModel):
    """Represents a single customer row."""

    customer_id: int
    region: str
    customer_type: str
    name: str
    genre: str


class SalesRecord(BaseModel):
    """Represents a single sales row."""

    product_id: int
    customer_id: int
    sold_on_date: int  # format YYYYMM
    sales_volume: float
    price: float
    cost: float


# ----------------------------------------
# Table Creation
# ----------------------------------------


def create_tables(db_path: str = "beverage_analysis.db") -> None:
    """
    Creates the 'product', 'customer', and 'sales' tables in DuckDB, if they don't already exist.
    :param db_path: Path to the DuckDB database file.
    """
    try:
        with duckdb.connect(database=db_path, read_only=False) as conn:
            # 1) PRODUCT TABLE
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS product (
                    product_id  INTEGER,   -- int4 in the spec
                    supplier    VARCHAR,      -- varchar in the spec
                    brand       VARCHAR,
                    family      VARCHAR,
                    color       VARCHAR,
                    name        VARCHAR
                );
            """
            )

            # 2) CUSTOMER TABLE
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS customer (
                    customer_id    INTEGER,  -- int4 in the spec
                    region         VARCHAR,     -- varchar in the spec
                    customer_type  VARCHAR,     -- varchar in the spec
                    name           VARCHAR,
                    genre          VARCHAR      -- comma-separated string for "ListEnum"
                );
            """
            )

            # 3) SALES TABLE
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sales (
                    product_id     INTEGER,  -- int4 in the spec
                    customer_id    BIGINT,   -- int8 in the spec
                    sold_on_date   INTEGER,  -- int4 (YYYYMM format)
                    sales_volume   DOUBLE,   -- float64
                    price          DOUBLE,   -- float64
                    cost           DOUBLE    -- float64
                );
            """
            )

        print("Tables 'product', 'customer', and 'sales' created or already exist.")
    except Exception as ex:
        print(f"[Error] Failed to create tables: {ex}")


# ----------------------------------------
# Record Generation Helpers
# ----------------------------------------
def generate_product_records(num_products: int = 30) -> List[ProductRecord]:
    """
    Generates a list of validated ProductRecord objects using random selections.
    :param num_products: Number of product records to generate.
    :return: List of ProductRecord models.
    """
    suppliers = ["Acme Inc", "PepsiCo", "CocaCola", "DrPepper", "Nestle"]
    brands = ["Now", "Classic", "HealthPlus", "Spark", "Retro"]
    families = ["Wine", "Spirits", "Beer", "Kombucha", "Pop", "Elixir"]
    colors = ["Red", "Green", "Blue Sky", "Teal", "Baby Pink", "Yellow"]

    records = []
    for i in range(num_products):
        product_id = i + 1
        supplier = random.choice(suppliers)
        brand = random.choice(brands)
        family = random.choice(families)
        color = random.choice(colors)
        name = f"{supplier} {brand} {family} {color}"

        try:
            record = ProductRecord(
                product_id=product_id,
                supplier=supplier,
                brand=brand,
                family=family,
                color=color,
                name=name,
            )
            records.append(record)
        except ValidationError as v_err:
            print(f"[Warning] Invalid ProductRecord: {v_err}")

    return records


def generate_customer_records(num_customers: int = 20) -> List[CustomerRecord]:
    """
    Generates a list of validated CustomerRecord objects using Faker and random selections.
    :param num_customers: Number of customer records to generate.
    :return: List of CustomerRecord models.
    """
    faker = Faker()
    Faker.seed(42)

    regions = [
        "California",
        "NewYork",
        "Ontario",
        "Quebec",
        "Texas",
        "Alberta",
        "BritishColumbia",
        "Illinois",
        "Florida",
        "Washington",
    ]
    customer_types = [
        "Store",
        "Grocery",
        "Box",
        "Restaurant",
        "PopUp",
        "Food Truck",
        "Transportation",
    ]
    possible_genres = [
        "Italian",
        "Indian",
        "Indonesian",
        "Ethiopian",
        "French",
        "Japanese",
        "American",
        "Spanish",
        "Mediterranean",
        "Chinese",
        "Eastern European",
        "North European",
        "Persian",
    ]

    records = []
    for i in range(num_customers):
        customer_id = i + 1
        region = random.choice(regions)
        ctype = random.choice(customer_types)
        name = faker.company()
        num_genre = random.randint(1, 3)
        selected_genres = random.sample(possible_genres, num_genre)
        genre_str = ",".join(selected_genres)

        try:
            record = CustomerRecord(
                customer_id=customer_id,
                region=region,
                customer_type=ctype,
                name=name,
                genre=genre_str,
            )
            records.append(record)
        except ValidationError as v_err:
            print(f"[Warning] Invalid CustomerRecord: {v_err}")

    return records


def generate_sales_records(
    num_sales: int = 50, max_product_id: int = 30, max_customer_id: int = 20
) -> List[SalesRecord]:
    """
    Generates a list of validated SalesRecord objects.
    :param num_sales: Number of sales records to generate.
    :param max_product_id: Maximum product ID (used for random selection).
    :param max_customer_id: Maximum customer ID (used for random selection).
    :return: List of SalesRecord models.
    """
    date_options = [202401, 202402, 202403, 202404, 202405]
    records = []
    for _ in range(num_sales):
        product_id = random.randint(1, max_product_id)
        customer_id = random.randint(1, max_customer_id)
        sold_on_date = random.choice(date_options)
        sales_volume = round(random.uniform(0.5, 20.0), 2)
        price = round(sales_volume * random.uniform(1.5, 3.5), 2)
        cost = round(price * random.uniform(0.4, 0.7), 2)

        try:
            record = SalesRecord(
                product_id=product_id,
                customer_id=customer_id,
                sold_on_date=sold_on_date,
                sales_volume=sales_volume,
                price=price,
                cost=cost,
            )
            records.append(record)
        except ValidationError as v_err:
            print(f"[Warning] Invalid SalesRecord: {v_err}")

    return records


# ----------------------------------------
# Populate Tables
# ----------------------------------------
def populate_tables(
    db_path: str = "beverage_analysis.db",
    num_products: int = 30,
    num_customers: int = 20,
    num_sales: int = 50,
) -> None:
    """
    Inserts validated fake data into the DuckDB 'product', 'customer', and 'sales' tables.
    :param db_path: Path to the DuckDB database file.
    :param num_products: Number of product records to generate.
    :param num_customers: Number of customer records to generate.
    :param num_sales: Number of sales transactions to generate.
    """
    product_records = generate_product_records(num_products=num_products)
    customer_records = generate_customer_records(num_customers=num_customers)
    sales_records = generate_sales_records(
        num_sales=num_sales, max_product_id=num_products, max_customer_id=num_customers
    )

    try:
        with duckdb.connect(database=db_path, read_only=False) as conn:
            # Insert product records
            conn.executemany(
                """
                INSERT INTO product (product_id, supplier, brand, family, color, name)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (r.product_id, r.supplier, r.brand, r.family, r.color, r.name)
                    for r in product_records
                ],
            )

            # Insert customer records
            conn.executemany(
                """
                INSERT INTO customer (customer_id, region, customer_type, name, genre)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (r.customer_id, r.region, r.customer_type, r.name, r.genre)
                    for r in customer_records
                ],
            )

            # Insert sales records
            conn.executemany(
                """
                INSERT INTO sales (product_id, customer_id, sold_on_date, sales_volume, price, cost) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        r.product_id,
                        r.customer_id,
                        r.sold_on_date,
                        r.sales_volume,
                        r.price,
                        r.cost,
                    )
                    for r in sales_records
                ],
            )

        print("Data insertion complete for 'product', 'customer', and 'sales' tables.")
    except Exception as ex:
        print(f"[Error] Failed to populate tables: {ex}")


# ----------------------------------------
# Orchestrator
# ----------------------------------------
def init_db_and_populate(
    db_path: str = "beverage_analysis.db",
    num_products: int = 30,
    num_customers: int = 20,
    num_sales: int = 50,
) -> None:
    """
    Convenience function to create all tables and populate them in one go.
    :param db_path: Path to the DuckDB database file.
    :param num_products: Number of product records to generate.
    :param num_customers: Number of customer records to generate.
    :param num_sales: Number of sales transactions to generate.
    """
    create_tables(db_path=db_path)
    populate_tables(
        db_path=db_path,
        num_products=num_products,
        num_customers=num_customers,
        num_sales=num_sales,
    )
    print(f"Database initialization complete. Tables populated in '{db_path}'.")


if __name__ == "__main__":
    # Example usage: create and populate the DB with default sizes
    init_db_and_populate()
