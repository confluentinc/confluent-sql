"""Consume a Flink streaming query and periodically snapshot it into pandas DataFrames.

Unlike `pd.read_sql()` which expects a finite result set, streaming queries run
indefinitely. This example shows how to batch streaming results into DataFrames
for periodic analysis -- useful for monitoring dashboards, alerting, or
micro-batch analytics.

Uses the tables from the Confluent Cloud quickstart (orders, customers, products, clicks).

Requirements:
    uv add pandas confluent-sql
"""

import sys
import time

import pandas as pd

sys.path.insert(0, "..")
from _connection import get_connection

conn = get_connection()
try:
    with conn.closing_streaming_cursor(as_dict=True) as cursor:
        cursor.execute("SELECT * FROM `clicks`")

        batch: list[dict] = []
        batch_interval_secs = 5.0
        last_flush = time.time()

        print("Consuming streaming click events (Ctrl+C to stop)...")
        while cursor.may_have_results:
            rows = cursor.fetchmany(100)
            if rows:
                batch.extend(rows)

            # Flush the batch into a DataFrame on the interval.
            if time.time() - last_flush >= batch_interval_secs and batch:
                df = pd.DataFrame(batch)
                print(f"\n--- Batch: {len(df)} rows ---")
                print(df.describe())
                batch.clear()
                last_flush = time.time()
            elif not rows:
                time.sleep(0.1)
finally:
    conn.close()
