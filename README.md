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

### Basic Connection

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
    cloud_provider="aws"
)

# Create cursor and execute query
cursor = connection.cursor()
cursor.execute("SELECT customer_id, name FROM customers")

# Fetch results (includes changelog operations)
for row in cursor:
    print(row)  # e.g., ('+I', 1, 'John')

# Clean up
cursor.close()
connection.close()
```

Example:
```python
cursor.execute("SELECT customer_id, name FROM customers")
results = cursor.fetchall()
# Results: [('+I', 1, 'John'), ('+I', 2, 'Jane')]
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

```bash
# Set required environment variables
export FLINK_API_KEY="your-key"
export FLINK_API_SECRET="your-secret"
export ENV_ID="env-123456"
export ORG_ID="org-123456"
export COMPUTE_POOL_ID="lfcp-789012"
export FLINK_REGION="us-east-2"

# Run all tests
uv run pytest

# Run only unit tests
uv run pytest -m "not integration"

# Run only integration tests
uv run pytest -m integration

# Run with coverage
uv run pytest --cov=confluent_sql --cov-report=html
```
