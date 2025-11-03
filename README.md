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
connection.close() # This will also close all the cursors
```

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
