# confluent-sql

A DB-API v2 compliant Python driver for Confluent Cloud Flink SQL services.

## Overview

The `confluent-sql` library provides a standard DB-API v2 interface for connecting to and
executing SQL queries against Confluent Cloud Flink SQL services. This allows you to use
familiar database programming patterns with Confluent's streaming SQL capabilities.

## Status

This is pre-production code mainly developed as the lower level portion of a `dbt` adaptor for Confluent Cloud Flink, but is aimed to also be a reasonable standalone dbapi+ driver for python programs to interact with Confluent Flink SQL.

The behavior of snapshot-mode cursors, complying with dbapi semantics, are well stable. The streaming query extensions are more of a work in progress at this time. Feedback and suggestions are welcome!

> **⚠️ Early Access:** [Snapshot queries](https://docs.confluent.io/cloud/current/flink/concepts/snapshot-queries.html) on Confluent Cloud Flink SQL are currently in Early Access and may be subject to change. The driver defaults to snapshot mode for all queries unless streaming mode is explicitly requested.

## Prerequisites

- **Confluent Cloud account** with Flink environment
- **Active Flink compute pool** (must be pre-created)
- **Flink Compute Pool API credentials** for Flink SQL API access: a user or service account API key and secret for the compute pool (used as HTTP Basic Auth, for example via `flink_api_key` and `flink_api_secret`).

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
    flink_api_key="your-flink-api-key",
    flink_api_secret="your-flink-api-secret",
    organization_id="your-org-uuid",
    environment="env-123456",
    compute_pool_id="lfcp-789012",
    cloud_provider="aws",
    cloud_region="us-east-2",
    dbname="your-database-name"
)
```

Create a cursor and run a point-in-time `SNAPSHOT` query:

```python
cursor = connection.cursor()
cursor.execute("SELECT customer_id, name FROM customers")
```

Fetch results using `fetchone()`, `fetchmany()` and `fetchall()`:

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
cursor.close()
connection.close()
```

**Dictionary Result Rows:**

```python

...
cursor = connection.cursor(as_dict=True)
cursor.execute("SELECT customer_id, name, email FROM customers WHERE customer_id = %s", (123,))
row = cursor.fetchone()
print(row["customer_id"])  # Access by column name
```

**Streaming Queries:**

```python
import time

...

# Execute a streaming statement, runs and produces results indefinitely until
# we stop consuming its results or the statement is stopped or deleted via API ...
cursor = connection.streaming_cursor()
cursor.execute("SELECT * FROM orders_stream WHERE total > %s", (1000,))

while cursor.may_have_results:
    rows = cursor.fetchmany(10)
    if rows:
        for row in rows:
            print(row)
    else:
        time.sleep(0.1)
```

## Parameterized Statement and Flink to Python Value Support

This driver supports all Flink types, some with caveats. Please consult [the type support documentation](TYPES.md) for more details.

## DB-API Extensions

This driver extends the standard DB-API v2 interface with additional features:

- **Dictionary result rows** - Access columns by name instead of position
- **Streaming cursors** - Non-blocking result consumption from continuous queries
- **Changelog compression** - Automatic state management for aggregations and joins
- **Statement lifecycle management** - Named statements, labels, and resource management
- **Type system** - Full support for all Flink SQL types including streaming-specific types
- **Performance monitoring** - Built-in fetch metrics and introspection

## Architecture & How It Works

The `confluent-sql` driver communicates with Confluent Cloud Flink SQL through HTTP-based APIs. Unlike in traditional databases, statements are **first-class entities** on the server with their own lifecycle,
allowing features like:

- **Named statements** - Identify and recover queries across connections
- **Persistent execution** - Statements survive connection close and can be resumed
- **Batch management** - Label related statements for group operations

For an in-depth explanation of the HTTP architecture and statement lifecycle,
see **[ARCHITECTURE.md](ARCHITECTURE.md)**.

### Complete Documentation

For comprehensive documentation of all DB-API extensions, see **[DBAPI_EXTENSIONS.md](DBAPI_EXTENSIONS.md)**.

For detailed streaming query guidance, see **[STREAMING.md](STREAMING.md)**.

For type support and examples, see **[TYPES.md](TYPES.md)**.

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
