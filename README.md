# confluent-sql

A DB-API v2 compliant Python driver for Confluent Cloud Flink SQL services.

## Overview

The `confluent-sql` library provides a standard DB-API v2 interface for connecting to and
executing SQL queries against Confluent Cloud Flink SQL services. This allows you to use
familiar database programming patterns with Confluent's streaming SQL capabilities.

## Prerequisites

- **Confluent Cloud account** with Flink environment
- **Active compute pool** (must be pre-created)
- **API credentials** for Flink SQL API access

## Installation

```bash
# Using pip
pip install confluent-sql

# Using uv (recommended)
uv add confluent-sql
```

## Quick Start

Setup the connection:

```python
import confluent_sql

# Connect to Confluent Cloud Flink SQL
connection = confluent_sql.connect(
    api_key="your-flink-api-key",
    api_secret="your-flink-api-secret",
    environment="env-123456",
    compute_pool_id="lfcp-789012",
    region="us-east-2",
    organization_id="org-123456",
    cloud_provider="aws",
    dbname="your-database-name"
)
```

Create a cursor and run a query:

```python
cursor = connection.cursor()
cursor.execute("SELECT customer_id, name FROM customers")
```

Fetch results using fetchone, fetchmany and fetchall:

```python
print(cursor.fetchone())
print(cursor.fetchmany(2))
print(cursor.fetchall())
```

Fetch results using the cursor as an iterator:

```python
for row in cursor:
    print(row)
```

Clean up:

```python
cusor.close()
connection.close()
```

## DB-API Extensions

This driver extends the standard DB-API v2 interface with several powerful features designed for stream processing:

### Dictionary Result Rows

Return results as dictionaries with column names as keys instead of tuples:

```python
cursor = connection.cursor(as_dict=True)
cursor.execute("SELECT customer_id, name, email FROM customers WHERE customer_id = %s", (123,))
row = cursor.fetchone()
print(row["customer_id"])  # Access by column name
print(row["name"])
```

### Streaming Queries

By default, queries execute in **snapshot mode**, returning a finite point-in-time result set. However, Confluent Cloud Flink is fundamentally a streaming event database — the driver offers streaming execution mode to interact with open-ended result sets that continuously produce new rows:

```python
import time

# Create a streaming cursor for unbounded queries
cursor = connection.streaming_cursor()
# Start a statement which produces an open-ended append-only result set.
cursor.execute("SELECT * FROM orders_stream WHERE total > %s", (1000,))

# Poll for results as they arrive (non-blocking)
while cursor.may_have_results:
    rows = cursor.fetchmany(10)
    if rows:
        for row in rows:
            print(row)
    else:
        time.sleep(0.1)  # Wait before next poll
    ...

cursor.close()
```

Unlike snapshot queries that block until all results are retrieved, streaming queries are **non-blocking**—they return immediately with any currently available data, allowing you to poll at your own pace or integrate with async patterns.

### Changelog Streams with State Management

For stateful queries (GROUP BY, JOIN, aggregations), the driver provides automatic changelog state compression through its `changelog_compressor` class, available from a streaming cursor:

```python
import time

cursor = connection.streaming_cursor(as_dict=True)
# Execute a streaming query whose result set changes over time --
# result set rows may be added / removed / rewritten as new events
# are encountered.
cursor.execute("SELECT product_id, COUNT(*) as sales FROM orders GROUP BY product_id")

# Automatically maintain current state from changelog events
compressor = cursor.changelog_compressor()
for snapshot in compressor.snapshots():
    # Each snapshot is the current aggregated state
    for row in snapshot:
        print(f"Product {row['product_id']}: {row['sales']} sales")
    time.sleep(5)
    ....

cursor.close()
```

For comprehensive documentation on streaming queries, changelog processing, and advanced patterns, see [STREAMING.md](STREAMING.md).

## Parameterized Statement and Flink to Python Value Support

This driver supports all Flink types, some with caveats. Please consult [the type support documentation](TYPES.md) for more details.

## Development

### Setup

```bash
# Clone repository
git clone <repository-url>
cd confluent-sql

# Install uv if needed
pip install uv

# Install dependencies
uv sync

# Install in development mode
uv pip install -e .
```

### Running Tests

Set required environment variables for integration tests.
If any of the variables is not set, integration tests will be skipped.

```bash
export CONFLUENT_FLINK_API_KEY="your-key"
export CONFLUENT_FLINK_API_SECRET="your-secret"
export CONFLUENT_ENV_ID="env-123456"
export CONFLUENT_ORG_ID="org-123456"
export CONFLUENT_COMPUTE_POOL_ID="lfcp-789012"
export CONFLUENT_CLOUD_PROVIDER="aws"
export CONFLUENT_CLOUD_REGION="us-east-2"
export CONFLUENT_TEST_DBNAME="test-db"
```

Run tests:

```bash
uv run pytest
```
