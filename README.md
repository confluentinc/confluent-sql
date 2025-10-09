# confluent-sql

A DB-API v2 compliant Python driver for Confluent Cloud Flink SQL services.

## Overview

The `confluent-sql` library provides a standard DB-API v2 interface for connecting to and executing SQL queries against Confluent Cloud Flink SQL services. This allows you to use familiar database programming patterns with Confluent's streaming SQL capabilities.

## Features

- **DB-API v2 compliant interface** - Standard Python database programming patterns
- **Direct HTTP integration** - Uses `httpx` for reliable API communication
- **Streaming SQL support** - Execute queries against Flink streaming engines
- **Result set handling** - Fetch results with changelog operations (+I, -D, -U, +U)
- **Standard exception hierarchy** - DB-API v2 compliant error handling
- **Automatic resource cleanup** - Proper connection and cursor management

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
    region="us-east-2",                    # Required: your Flink region
    organization_id="org-123456",          # Required: your organization ID
    cloud_provider="aws"                   # Required: your cloud provider
)

# Create cursor and execute query
cursor = connection.cursor()
cursor.execute("SELECT 1 as value")

# Fetch results (includes changelog operations)
rows = cursor.fetchall()
for row in rows:
    print(row)  # e.g., ('+I', 1)

# Clean up
cursor.close()
connection.close()
```

### Environment Variables Setup

```bash
# Required environment variables
export FLINK_API_KEY="your-flink-api-key"
export FLINK_API_SECRET="your-flink-api-secret"
export ENV_ID="env-123456"
export ORG_ID="org-123456"
export COMPUTE_POOL_ID="lfcp-789012"
export FLINK_REGION="us-east-2"
```

Then use in your code:

```python
import os
import confluent_sql

connection = confluent_sql.connect(
    api_key=os.environ["FLINK_API_KEY"],
    api_secret=os.environ["FLINK_API_SECRET"],
    environment=os.environ["ENV_ID"],
    compute_pool_id=os.environ["COMPUTE_POOL_ID"],
    region=os.environ["FLINK_REGION"],
    organization_id=os.environ["ORG_ID"],
    cloud_provider="aws"
)
```

## API Reference

### Connection Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `api_key` | str | Yes | Flink API key for authentication |
| `api_secret` | str | Yes | Flink API secret for authentication |
| `environment` | str | Yes | Environment ID (e.g., "env-123456") |
| `compute_pool_id` | str | Yes | Compute pool ID (e.g., "lfcp-789012") |
| `region` | str | Yes | Cloud region (e.g., "us-east-2", "us-west-2") |
| `organization_id` | str | Yes | Organization ID |
| `cloud_provider` | str | Yes | Cloud provider (e.g., "aws", "gcp", "azure") |
| `host` | str | No | Custom host URL (auto-constructed if not provided) |
| `test_mode` | bool | No | Enable test mode for development (default: False) |

### Cursor Methods

| Method | Description |
|--------|-------------|
| `execute(operation, parameters=None)` | Execute SQL statement |
| `fetchall()` | Fetch all results |
| `fetchone()` | Fetch next result row |
| `fetchmany(size=None)` | Fetch multiple result rows |
| `close()` | Close cursor and free resources |

### Result Format

Results include changelog operations as the first column:

- `+I` - Insert operation
- `-D` - Delete operation  
- `-U` - Update before (old values)
- `+U` - Update after (new values)

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

### Test Coverage

The test suite covers:

- ✅ **Connection Management** - Connection establishment and cleanup
- ✅ **SQL Execution** - Statement submission and status tracking
- ✅ **Result Fetching** - All DB-API fetch methods
- ✅ **Error Handling** - Invalid operations and API errors
- ✅ **Resource Cleanup** - Proper cursor and connection cleanup
- ✅ **DB-API Compliance** - All required DB-API v2 methods

## Architecture

The driver uses a simplified architecture:

- **Direct HTTP calls** using `httpx` client
- **Flink SQL API integration** for statement management
- **DB-API v2 compliance** with standard Python database patterns
- **No external dependencies** beyond standard HTTP libraries

## Common Issues

### Authentication Errors
- Ensure you're using valid Flink API credentials
- Verify the API key has access to the specified environment

### Region Mismatch
- Confirm the `region` parameter matches your Flink environment
- Check that `cloud_provider` is correct

### Compute Pool Issues
- Verify the compute pool exists and is active
- Ensure your credentials have access to the compute pool

## License

[License information to be added]

## Contributing

[Contribution guidelines to be added]
