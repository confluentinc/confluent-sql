"""LangChain SQL agent that queries Confluent Flink SQL via natural language.

Uses confluent-sql as the DB-API backend so an LLM can explore and query
Flink tables (backed by Kafka topics) conversationally.

Requirements:
    uv add langchain langchain-community langchain-anthropic langgraph confluent-sql
"""

import os
import sys

from langchain_anthropic import ChatAnthropic
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent

sys.path.insert(0, "..")
from _connection import get_connection


def create_flink_sql_agent():
    """Build a LangChain SQL agent backed by Confluent Flink SQL."""
    conn = get_connection()

    llm = ChatAnthropic(
        model="claude-sonnet-4-20250514",
        api_key=os.environ["ANTHROPIC_API_KEY"],
    )

    @tool
    def run_flink_sql(query: str) -> str:
        """Execute a Flink SQL query and return the results as a formatted string.
        Use this to query Kafka topics and Flink tables.
        Always use LIMIT to avoid huge result sets."""
        with conn.closing_cursor(as_dict=True) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            if not rows:
                return "No results."
            columns = list(rows[0].keys())
            header = " | ".join(columns)
            lines = [header, "-" * len(header)]
            for row in rows[:50]:
                lines.append(" | ".join(str(row[c]) for c in columns))
            return "\n".join(lines)

    @tool
    def list_flink_tables() -> str:
        """List all available tables (Kafka topics) in the current Flink database."""
        with conn.closing_cursor(as_dict=True) as cursor:
            cursor.execute("SHOW TABLES")
            rows = cursor.fetchall()
            if not rows:
                return "No tables found."
            return "\n".join(str(row) for row in rows)

    @tool
    def describe_flink_table(table_name: str) -> str:
        """Show the schema (columns and types) for a Flink table."""
        with conn.closing_cursor(as_dict=True) as cursor:
            cursor.execute(f"DESCRIBE `{table_name}`")
            rows = cursor.fetchall()
            if not rows:
                return f"Table '{table_name}' not found."
            return "\n".join(str(row) for row in rows)

    agent = create_react_agent(
        llm,
        tools=[run_flink_sql, list_flink_tables, describe_flink_table],
        prompt=(
            "You are a data analyst with access to Confluent Cloud Flink SQL. "
            "You can query Kafka topics as SQL tables. Use Flink SQL syntax. "
            "Always start by listing tables and describing their schema before querying. "
            "Use LIMIT clauses to keep result sets manageable."
        ),
    )
    return agent, conn


if __name__ == "__main__":
    agent, conn = create_flink_sql_agent()
    try:
        print("Flink SQL Agent ready. Type your questions (Ctrl+C to exit).\n")
        while True:
            question = input("You: ").strip()
            if not question:
                continue
            result = agent.invoke({"messages": [{"role": "user", "content": question}]})
            for msg in reversed(result["messages"]):
                if msg.type == "ai" and msg.content:
                    print(f"\nAgent: {msg.content}\n")
                    break
    except KeyboardInterrupt:
        print("\nGoodbye!")
    finally:
        conn.close()
