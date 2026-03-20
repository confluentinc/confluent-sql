"""Read Confluent Flink SQL tables directly into pandas DataFrames.

pandas accepts any DB-API v2 connection via `pd.read_sql()`, which means
confluent-sql works out of the box -- no extra adapters needed.

Uses the tables from the Confluent Cloud quickstart (orders, customers, products, clicks).

Requirements:
    uv add pandas confluent-sql
"""

import sys

import pandas as pd

sys.path.insert(0, "..")
from _connection import get_connection

conn = get_connection()
try:
    # List all available tables (Kafka topics) in the Flink database.
    tables = pd.read_sql("SHOW TABLES", conn)
    print(f"Available tables ({len(tables)}):")
    print(tables.to_string())

    # Read a Flink table (backed by a Kafka topic) into a DataFrame.
    # This runs a snapshot query -- point-in-time, bounded results.
    print("\n--- Orders ---")
    orders = pd.read_sql("SELECT * FROM `orders` LIMIT 20", conn)
    print(orders)
    print(f"\nShape: {orders.shape}")
    print(f"Dtypes:\n{orders.dtypes}")

    # Aggregation works too -- Flink runs the GROUP BY over the Kafka topic.
    print("\n--- Revenue by product ---")
    revenue = pd.read_sql(
        """
        SELECT
            product_id,
            COUNT(*) AS order_count,
            SUM(price) AS total_revenue
        FROM `orders`
        GROUP BY product_id
        ORDER BY total_revenue DESC
        """,
        conn,
    )
    print(revenue.to_string())
finally:
    conn.close()
