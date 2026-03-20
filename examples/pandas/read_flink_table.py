"""Read Confluent Flink SQL tables directly into pandas DataFrames.

pandas accepts any DB-API v2 connection via `pd.read_sql()`, which means
confluent-sql works out of the box -- no extra adapters needed.

Requirements:
    uv add pandas confluent-sql
"""

import sys

import pandas as pd

sys.path.insert(0, "..")
from _connection import get_connection

conn = get_connection()
try:
    # Read a Flink table (backed by a Kafka topic) into a DataFrame.
    # This runs a snapshot query -- point-in-time, bounded results.
    df = pd.read_sql("SELECT * FROM orders LIMIT 100", conn)
    print(df)
    print(f"\nShape: {df.shape}")
    print(f"Dtypes:\n{df.dtypes}")

    # You can use parameterized queries too.
    df_filtered = pd.read_sql(
        "SELECT * FROM orders WHERE customer_id = %s",
        conn,
        params=("customer-42",),
    )
    print(f"\nFiltered results: {len(df_filtered)} rows")

    # Use the DataFrame for analysis as usual.
    if "amount" in df.columns:
        print(f"\nTotal amount: {df['amount'].sum()}")
        print(f"Average amount: {df['amount'].mean():.2f}")
finally:
    conn.close()
