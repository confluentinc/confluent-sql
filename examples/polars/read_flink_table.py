"""Read Confluent Flink SQL tables into Polars DataFrames.

Polars can read from any DB-API v2 connection via `pl.read_database()`.

Uses the tables from the Confluent Cloud quickstart (orders, customers, products, clicks).

Requirements:
    uv add polars confluent-sql
"""

import sys

import polars as pl

sys.path.insert(0, "..")
from _connection import get_connection

conn = get_connection()
try:
    # Read a Flink table into a Polars DataFrame using the DB-API connection.
    df = pl.read_database("SELECT * FROM `orders` LIMIT 50", conn)
    print(df)
    print(f"\nShape: {df.shape}")
    print(f"Schema: {df.schema}")

    # Polars operations work as expected.
    if "product_id" in df.columns and "price" in df.columns:
        print("\n--- Revenue by product ---")
        summary = df.group_by("product_id").agg(
            pl.col("price").sum().alias("total_revenue"),
            pl.col("price").mean().alias("avg_price"),
            pl.len().alias("order_count"),
        ).sort("total_revenue", descending=True)
        print(summary)
finally:
    conn.close()
