"""Read Confluent Flink SQL tables into Polars DataFrames.

Polars can read from any DB-API v2 connection via `pl.read_database()`.

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
    df = pl.read_database("SELECT * FROM orders LIMIT 100", conn)
    print(df)
    print(f"\nShape: {df.shape}")
    print(f"Schema:\n{df.schema}")

    # Polars operations work as expected.
    if "amount" in df.columns:
        summary = df.group_by("customer_id").agg(
            pl.col("amount").sum().alias("total"),
            pl.col("amount").mean().alias("avg"),
            pl.len().alias("count"),
        )
        print(f"\nPer-customer summary:\n{summary}")
finally:
    conn.close()
