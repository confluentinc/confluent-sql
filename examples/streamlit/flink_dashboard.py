"""Streamlit dashboard powered by Confluent Flink SQL.

A minimal but complete Streamlit app that queries Flink tables and renders
live charts. Uses the Confluent Cloud quickstart tables.

Queries run in parallel using concurrent.futures for faster page loads.

Run with:
    streamlit run flink_dashboard.py

Requirements:
    uv add streamlit pandas confluent-sql
"""

import sys
from concurrent.futures import ThreadPoolExecutor, Future

import pandas as pd
import streamlit as st

sys.path.insert(0, "..")
from _connection import get_connection

st.set_page_config(page_title="Flink SQL Dashboard", layout="wide")
st.title("Confluent Flink SQL Dashboard")


def run_query(sql: str) -> pd.DataFrame:
    """Run a snapshot query in its own connection and return a DataFrame."""
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


QUERIES = {
    "orders": "SELECT * FROM `orders` LIMIT 20",
    "by_customer": """
        SELECT
            customer_id,
            COUNT(*) AS order_count
        FROM `orders`
        GROUP BY customer_id
        ORDER BY order_count DESC
    """,
    "tables": "SHOW TABLES",
}

# Fire all queries in parallel.
with ThreadPoolExecutor(max_workers=len(QUERIES)) as pool:
    futures: dict[str, Future] = {
        name: pool.submit(run_query, sql) for name, sql in QUERIES.items()
    }

# --- Sidebar: ad-hoc query editor ---
st.sidebar.header("Query Editor")
custom_sql = st.sidebar.text_area(
    "Flink SQL",
    value="SELECT * FROM `orders` LIMIT 20",
    height=120,
)
if st.sidebar.button("Run"):
    with st.spinner("Querying Flink..."):
        try:
            result = run_query(custom_sql)
            st.subheader("Query Results")
            st.dataframe(result, use_container_width=True)
        except Exception as e:
            st.error(f"Query failed: {e}")

# --- Main area: pre-built views ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Recent Orders")
    try:
        st.dataframe(futures["orders"].result(), use_container_width=True)
    except Exception as e:
        st.error(f"Query failed: {e}")

with col2:
    st.subheader("Orders by Customer")
    try:
        st.bar_chart(futures["by_customer"].result(), x="customer_id", y="order_count")
    except Exception as e:
        st.error(f"Query failed: {e}")

# --- Tables list ---
st.subheader("Available Tables")
try:
    st.dataframe(futures["tables"].result(), use_container_width=True)
except Exception as e:
    st.error(f"Query failed: {e}")
