"""Streamlit dashboard powered by Confluent Flink SQL.

A minimal but complete Streamlit app that queries Flink tables and renders
live charts. Run with:

    streamlit run flink_dashboard.py

Requirements:
    uv add streamlit pandas confluent-sql
"""

import sys

import pandas as pd
import streamlit as st

sys.path.insert(0, "..")
from _connection import get_connection

st.set_page_config(page_title="Flink SQL Dashboard", layout="wide")
st.title("Confluent Flink SQL Dashboard")


@st.cache_resource
def _get_connection():
    return get_connection()


@st.cache_data(ttl=30)
def run_query(sql: str) -> pd.DataFrame:
    """Run a snapshot query and return a DataFrame. Results are cached for 30s."""
    conn = _get_connection()
    return pd.read_sql(sql, conn)


# --- Sidebar: ad-hoc query editor ---
st.sidebar.header("Query Editor")
custom_sql = st.sidebar.text_area(
    "Flink SQL",
    value="SELECT * FROM orders LIMIT 50",
    height=120,
)
if st.sidebar.button("Run"):
    with st.spinner("Querying Flink..."):
        result = run_query(custom_sql)
    st.subheader("Query Results")
    st.dataframe(result, use_container_width=True)

# --- Main area: pre-built views ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Recent Orders")
    try:
        orders = run_query("SELECT * FROM orders LIMIT 20")
        st.dataframe(orders, use_container_width=True)
    except Exception as e:
        st.error(f"Query failed: {e}")

with col2:
    st.subheader("Order Volume")
    try:
        volume = run_query(
            """
            SELECT
                customer_id,
                COUNT(*) AS order_count
            FROM orders
            GROUP BY customer_id
            """
        )
        st.bar_chart(volume, x="customer_id", y="order_count")
    except Exception as e:
        st.error(f"Query failed: {e}")
