# DB-API Extensions

The `confluent-sql` driver extends the standard [DB-API v2](https://peps.python.org/pep-0249/) interface with features specifically designed for stream processing and event-driven applications with Confluent Cloud Flink SQL.

## Understanding Snapshot vs Streaming Modes

**By default, the driver operates in [SNAPSHOT mode](https://docs.confluent.io/cloud/current/flink/concepts/snapshot-queries.html)**, producing behavior very similar to traditional SQL databases:

- Queries execute and block until complete
- Results are finite (complete as of query start time)
- Standard DB-API methods (`fetchone()`, `fetchall()`, iteration) work as expected
- Perfect for ad-hoc data exploration and analysis
- No special streaming knowledge required for simple queries

**However, Confluent Cloud Flink is fundamentally a streaming database.** The driver provides optional **streaming mode** for applications that need to work with continuous, unbounded data:

- Queries run indefinitely, producing results as they arrive
- Non-blocking fetch methods (polling pattern with `may_have_results`)
- Stateful queries return changelog events tracking row modifications
- Automatic state compression available via `changelog_compressor()`

This design choice means:

- ✅ **Familiar to traditional database users** - Snapshot mode works like PostgreSQL/MySQL/SQLite
- ✅ **Opt-in streaming** - Enable it only when you need continuous data
- ✅ **Pure DB-API compliance** - Snapshot queries don't require any extensions

All extensions are **backward compatible** and **opt-in**—standard DB-API code works unchanged.

For comprehensive details on streaming queries, polling patterns, and changelog handling, see **[STREAMING.md](STREAMING.md)**.

## Quick Navigation

- [Result Format Extensions](#result-format-extensions) - Dictionary rows, custom types
- [Streaming Query Support](#streaming-query-support) - Comprehensive streaming guide
- [Statement Lifecycle Management](#statement-lifecycle-management) - DDL, naming, deletion
- [Introspection and Metadata](#introspection-and-metadata) - Properties for query state
- [Performance Monitoring](#performance-monitoring) - Fetch metrics
- [Type System Extensions](#type-system-extensions) - Flink type support
- [Custom Exceptions](#custom-exceptions) - Streaming-specific errors
- [Extended Execute Parameters](#extended-execute-parameters) - Additional cursor.execute() options

---

## Result Format Extensions

These extensions control how results are returned by cursors, and work with both **snapshot queries** (finite results) and **streaming queries** (continuous results).

### Dictionary Result Rows (`as_dict=True`)

By default, cursors return rows as tuples (standard DB-API). Use `as_dict=True` to return dictionaries with column names as keys, improving readability and enabling column access by name.

**Available on:**

- `connection.cursor(as_dict=True)`
- `connection.streaming_cursor(as_dict=True)`
- `connection.closing_cursor(as_dict=True)`
- `connection.closing_streaming_cursor(as_dict=True)`

**Example:**

```python
# Tuple access (default)
cursor = connection.cursor()
cursor.execute("SELECT customer_id, name, email FROM customers")
row = cursor.fetchone()
print(row[0])  # customer_id by position
print(row[1])  # name

# Dictionary access
cursor = connection.cursor(as_dict=True)
cursor.execute("SELECT customer_id, name, email FROM customers")
row = cursor.fetchone()
print(row["customer_id"])  # Access by column name
print(row["name"])
```

**Use dictionary rows when:**

- ✅ Column names improve code readability
- ✅ Working with many columns (easier to track positions)
- ✅ Passing rows to functions expecting dicts
- ✅ JSON serialization needed
- ✅ Code is less brittle to schema changes

**Use tuple rows when:**

- ✅ Performance is critical (tuples are marginally faster)
- ✅ Position-based access is clearer for your use case
- ✅ Minimal memory overhead needed

---

### Auto-Closing Cursor (`closing_cursor()`)

Convenience context manager that creates and automatically closes a cursor, simplifying resource management.

**Signature:**

```python
with connection.closing_cursor(as_dict=False, mode=ExecutionMode.SNAPSHOT) as cursor:
    cursor.execute("SELECT * FROM users")
    for row in cursor:
        print(row)
# cursor automatically closed
```

**Equivalent to:**

```python
cursor = connection.cursor(as_dict=False, mode=ExecutionMode.SNAPSHOT)
try:
    cursor.execute("SELECT * FROM users")
    for row in cursor:
        print(row)
finally:
    cursor.close()
```

**Benefits:**

- ✅ Guarantees cursor cleanup even if exceptions occur
- ✅ Cleaner, more Pythonic code
- ✅ Works with streaming and snapshot cursors

**Example with Streaming Cursor:**

```python
import time
from confluent_sql.execution_mode import ExecutionMode

with connection.closing_cursor(mode=ExecutionMode.STREAMING_QUERY, as_dict=True) as cursor:
    cursor.execute("SELECT * FROM orders_stream WHERE total > %s", (1000,))

    while cursor.may_have_results:
        rows = cursor.fetchmany(10)
        if rows:
            for row in rows:
                print(f"Order: {row['order_id']}, Total: {row['total']}")
        else:
            time.sleep(0.1)
# cursor automatically closed, statement cleanup handled
```

---

### Auto-Closing Streaming Cursor (`closing_streaming_cursor()`)

Convenience context manager specifically for streaming cursors—equivalent to `closing_cursor(mode=ExecutionMode.STREAMING_QUERY, as_dict=as_dict)`. This is the recommended way to create auto-closing streaming cursors.

**Signature:**

```python
with connection.closing_streaming_cursor(as_dict=True) as cursor:
    cursor.execute("SELECT * FROM orders_stream WHERE total > %s", (1000,))
    while cursor.may_have_results:
        rows = cursor.fetchmany(10)
        if rows:
            for row in rows:
                process(row)
# cursor automatically closed
```

**Equivalent to:**

```python
import confluent_sql

cursor = connection.closing_cursor(as_dict=True, mode=confluent_sql.ExecutionMode.STREAMING_QUERY)
try:
    cursor.execute("SELECT * FROM orders_stream WHERE total > %s", (1000,))
    while cursor.may_have_results:
        rows = cursor.fetchmany(10)
        if rows:
            for row in rows:
                process(row)
finally:
    cursor.close()
```

**Benefits:**

- ✅ More concise than `closing_cursor()` with explicit mode parameter
- ✅ Intent is immediately clear: "streaming cursor"
- ✅ No need to import `ExecutionMode`
- ✅ Guarantees cursor cleanup even if exceptions occur

**Complete Example:**

```python
import time
import confluent_sql

connection = confluent_sql.connect(...)

min_amount = 1000
with connection.closing_streaming_cursor(as_dict=True) as cursor:
    cursor.execute("""
        SELECT order_id, customer_id, amount FROM orders
        WHERE amount > %s
    """, (min_amount,))

    start_time = time.time()
    timeout_seconds = 60

    while cursor.may_have_results:
        rows = cursor.fetchmany(10)
        if rows:
            for row in rows:
                print(f"Order {row['order_id']}: ${row['amount']} from customer {row['customer_id']}")
        else:
            if time.time() - start_time > timeout_seconds:
                print("No new orders for 60 seconds, exiting")
                break
            time.sleep(0.5)
# cursor automatically closed, statement cleanup handled
```

**When to use:**

- ✅ Preferred for all streaming cursor usage (cleaner than `closing_cursor()`)
- ✅ Processing continuous data from Flink SQL
- ✅ Non-blocking event consumption with automatic resource cleanup

For detailed streaming query patterns and examples, see [STREAMING.md](STREAMING.md).

---

### Custom ROW Type Mapping (`register_row_type()`)

Register custom `namedtuple`, `typing.NamedTuple`, or `@dataclass` classes to be used when deserializing Flink ROW types in query results.

**Signature:**

```python
connection.register_row_type(MyRowClass)
```

**Example:**

```python
from typing import NamedTuple
from collections import namedtuple

# Define custom row type
class OrderRow(NamedTuple):
    order_id: int
    customer_name: str
    total_amount: float

# Register with connection
connection.register_row_type(OrderRow)

# Now ROW results will use this type
with connection.closing_cursor(as_dict=True) as cursor:
    cursor.execute("SELECT order_row FROM orders")
    row = cursor.fetchone()
    order = row["order_row"]
    assert isinstance(order, OrderRow)
    print(f"Order {order.order_id}: {order.customer_name}")
```

**When to use:**

- ✅ Type safety for ROW columns
- ✅ IDE autocomplete for ROW field access
- ✅ Validation and custom methods on ROW types

For detailed type support documentation and more complex examples, see [TYPES.md](TYPES.md).

---

## Streaming Query Support

While the driver defaults to [SNAPSHOT mode](https://docs.confluent.io/cloud/current/flink/concepts/snapshot-queries.html) (traditional DB-API behavior), Confluent Cloud Flink is fundamentally a **streaming event database**. The driver provides full support for continuous streaming queries through extended cursor types and execution modes.

**Key Distinction:**

- **[SNAPSHOT mode](https://docs.confluent.io/cloud/current/flink/concepts/snapshot-queries.html)** is designed for users migrating from PostgreSQL, MySQL, etc. Write standard DB-API code and get traditional database behavior.
- **STREAMING_QUERY mode** unlocks Flink's streaming capabilities: continuous results, non-blocking fetches, changelog streams, and state management.

### Execution Modes

| Mode                                                                                                   | Method                          | Use Case                        | Result Behavior          | Fetch Pattern                                                                |
| ------------------------------------------------------------------------------------------------------ | ------------------------------- | ------------------------------- | ------------------------ | ---------------------------------------------------------------------------- |
| **[SNAPSHOT](https://docs.confluent.io/cloud/current/flink/concepts/snapshot-queries.html)** (default) | `connection.cursor()`           | Point-in-time queries (bounded) | Finite result set        | Blocking: `fetchall()` works, `for row in cursor` blocks                     |
| **STREAMING_QUERY**                                                                                    | `connection.streaming_cursor()` | Continuous queries (unbounded)  | Results arrive over time | Non-blocking: Poll with `fetchone()`/`fetchmany()`, check `may_have_results` |

### Quick Example

```python
import time
from contextlib import closing

# Create streaming cursor
cursor = connection.streaming_cursor(as_dict=True)

# Execute streaming query
cursor.execute("SELECT order_id, amount FROM orders WHERE amount > %s", (1000,))

# Poll for results (non-blocking)
while cursor.may_have_results:
    rows = cursor.fetchmany(10)
    if rows:
        for row in rows:
            print(f"Order {row['order_id']}: ${row['amount']}")
    else:
        time.sleep(0.5)  # Wait before next poll
```

### Complete Streaming Documentation

For comprehensive documentation of streaming queries, see **[STREAMING.md](STREAMING.md)**.

---

## Statement Lifecycle Management

### DDL Execution Convenience Methods

Execute Data Definition Language statements (CREATE TABLE, ALTER, DROP, etc.) with appropriate execution modes and resource management.

**`execute_snapshot_ddl()` - Bounded DDL**

```python
statement = connection.execute_snapshot_ddl(
    "CREATE TABLE users AS SELECT * FROM source_users WHERE created_date > %s",
    (date(2024, 1, 1),),
    timeout=3000  # Wait up to 3000 seconds
)
print(f"Created table, statement: {statement.name}")
```

Use for DDL operations that complete after processing finite data:

- `CREATE TABLE` (not as SELECT)
- `CREATE TABLE AS SELECT` (snapshot mode, bounded source)
- `DROP TABLE`
- `ALTER TABLE`
- `CREATE VIEW`

**`execute_streaming_ddl()` - Unbounded/Open-Ended Statements**

Despite the name, this method is not limited to DDL statements. Use it for any open-ended statement that produces no results back to the client and runs indefinitely—including DDL operations and data ingestion/transformation jobs.

```python
# Create a table from streaming source
statement = connection.execute_streaming_ddl(
    "CREATE TABLE orders_stream AS SELECT * FROM kafka_orders",
    timeout=3000  # Wait for job to start (doesn't wait for completion)
)
print(f"Started streaming job: {statement.name}")
```

```python
# Continuously ingest and transform data
statement = connection.execute_streaming_ddl(
    """
    INSERT INTO filtered_orders
    SELECT order_id, customer_id, amount
    FROM orders_kafka_source
    WHERE amount > %s
    """,
    (100,),
    timeout=3000
)
print(f"Started data pipeline: {statement.name}")
```

Use for statements that produce unbounded/continuous results:

- `CREATE TABLE AS SELECT` from streaming sources
- `INSERT INTO ... SELECT` from continuous sources (data pipelines)
- Any DDL or DML producing indefinitely running Flink jobs

**Benefits vs manual cursor approach:**

- ✅ Clearer intent in code
- ✅ Automatically sets correct execution mode
- ✅ No need to create/manage cursor for one-off DDL
- ✅ Returns Statement object for management

---

### Statement Naming and Labeling

Control statement identity and grouping for tracking and management.

**Statement Names** - Unique identifier for each statement

```python
cursor.execute(
    "SELECT * FROM users WHERE active = %s",
    (True,),
    statement_name="active-users-query"
)
# Later, delete by name
connection.delete_statement("active-users-query")
```

**Statement Labels** - Group related statements for batch operations

```python
# Execute multiple statements with same label
for source in ["kafka_topic_a", "kafka_topic_b", "kafka_topic_c"]:
    cursor.execute(
        f"CREATE TABLE {source}_backup AS SELECT * FROM {source}",
        statement_label="daily-backups"
    )

# Later, list and delete all backups
statements = connection.list_statements(label="daily-backups")
for stmt in statements:
    connection.delete_statement(stmt)
```

**When to use:**

- ✅ Long-running streaming jobs (track and manage)
- ✅ Batch operations (group related statements)
- ✅ Error recovery (find and clean up failed jobs)

---

### Finding and Deleting Statements

**`list_statements()` - Find statements by label**

```python
# Find all statements with a specific label
statements = connection.list_statements(label="daily-backups", page_size=100)

for statement in statements:
    print(f"Statement: {statement.name}")
    print(f"Phase: {statement.phase}")  # RUNNING, COMPLETED, FAILED, etc.
    print(f"Created: {statement.created_at}")
```

**`delete_statement()` - Stop and remove a statement**

```python
# Delete from connection (by name or Statement object)
connection.delete_statement("active-users-query")
connection.delete_statement(statement_obj)

# Delete from cursor (current statement)
cursor.delete_statement()
```

**When to delete statements:**

- ✅ Long-running streaming jobs no longer needed
- ✅ Freeing compute pool resources (required before closing connection)
- ✅ Cleanup during error handling
- ⚠️ Deletion stops the statement immediately (may cause errors if still in use)

---

## Introspection and Metadata

### Connection Properties

| Property          | Type   | Description                                                       |
| ----------------- | ------ | ----------------------------------------------------------------- |
| `is_closed`       | `bool` | Check if connection has been closed                               |
| `http_user_agent` | `str`  | Get or set User-Agent header for HTTP requests (1-100 characters) |

**Example:**

```python
# Check if connection is still active
if not connection.is_closed:
    cursor = connection.cursor()
    cursor.execute("SELECT 1")

# Customize User-Agent
connection.http_user_agent = "MyApp/1.0 (custom agent)"
```

---

### Cursor Properties

| Property            | Type            | When Available    | Description                                            |
| ------------------- | --------------- | ----------------- | ------------------------------------------------------ |
| `statement`         | `Statement`     | After `execute()` | Full statement metadata and lifecycle info             |
| `may_have_results`  | `bool`          | After `execute()` | More data may arrive (streaming), or results exhausted |
| `is_closed`         | `bool`          | Always            | Check if cursor is closed                              |
| `execution_mode`    | `ExecutionMode` | After `execute()` | `SNAPSHOT` or `STREAMING_QUERY`                        |
| `is_streaming`      | `bool`          | After `execute()` | Convenience: `execution_mode == STREAMING_QUERY`       |
| `returns_changelog` | `bool`          | After `execute()` | Results are ChangeloggedRow with operations            |
| `as_dict`           | `bool`          | Always            | Rows returned as dicts vs tuples                       |

**Usage Examples:**

```python
cursor = connection.streaming_cursor(as_dict=True)
cursor.execute("SELECT * FROM orders GROUP BY customer_id")

# Introspect query characteristics
if cursor.returns_changelog:
    print("Query is stateful - use changelog compressor")
    compressor = cursor.changelog_compressor()
else:
    print("Query is append-only - iterate directly")

# Check execution mode
if cursor.is_streaming:
    print("Non-blocking fetch pattern")
    while cursor.may_have_results:
        row = cursor.fetchone()
        if row:
            process(row)
        else:
            time.sleep(0.1)
else:
    print("Blocking fetch pattern")
    for row in cursor:
        process(row)
```

---

### Statement Object

The `cursor.statement` property provides detailed metadata about the executed query.

**Key Statement Properties:**

| Property         | Type             | Description                                                             |
| ---------------- | ---------------- | ----------------------------------------------------------------------- |
| `name`           | `str`            | Statement identifier (auto-generated UUID or custom name)               |
| `phase`          | `StatementPhase` | Execution phase: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `STOPPED` |
| `is_append_only` | `bool`           | Query produces only inserts (vs changelog with updates/deletes)         |
| `is_bounded`     | `bool`           | Query has finite result set (snapshot) vs unbounded (streaming)         |
| `is_deletable`   | `bool`           | Statement can be deleted                                                |
| `schema`         | `Schema`         | Result schema with column names and types                               |
| `sql_kind`       | `str`            | Query type: `SELECT`, `INSERT`, `CREATE`, etc.                          |

**Example:**

```python
cursor.execute("SELECT product_id, COUNT(*) as sales FROM orders GROUP BY product_id")

stmt = cursor.statement
print(f"Statement: {stmt.name}")
print(f"SQL Kind: {stmt.sql_kind}")
print(f"Schema: {stmt.schema}")

# Check query characteristics
print(f"Append-only: {stmt.is_append_only}")
print(f"Bounded: {stmt.is_bounded}")
```

---

## Performance Monitoring

### Fetch Metrics

Monitor and analyze result fetching performance using the `cursor.metrics` property. Useful for identifying bottlenecks and optimizing polling patterns in streaming applications.

**Basic Example:**

```python
cursor = connection.streaming_cursor()
cursor.execute("SELECT * FROM high_volume_stream")

# Consume some results
for _ in range(100):
    row = cursor.fetchone()
    if not row:
        time.sleep(0.1)

# Analyze performance
metrics = cursor.metrics
print(f"Total page fetches: {metrics.total_page_fetches}")
print(f"Rows returned: {metrics.rows_returned}")
print(f"Time in fetches: {metrics.fetch_request_secs:.2f}s")
print(f"Total pause time: {metrics.paused_secs:.2f}s")
print(f"Bytes received: {metrics.bytes_received}")
```

**Available Metrics:**

| Metric                         | Type    | Description                            |
| ------------------------------ | ------- | -------------------------------------- |
| `total_page_fetches`           | `int`   | Total result pages fetched from server |
| `total_changelog_rows_fetched` | `int`   | Total changelog rows received          |
| `empty_page_fetches`           | `int`   | Pages with no rows (polling overhead)  |
| `fetch_request_secs`           | `float` | Total time spent in fetch requests     |
| `paused_times`                 | `int`   | Number of times pause delay occurred   |
| `paused_secs`                  | `float` | Total time paused between fetches      |
| `bytes_received`               | `int`   | Total bytes received from server       |
| `rows_returned`                | `int`   | Total rows returned to caller          |

**Use cases:**

- Debugging slow queries
- Monitoring streaming query throughput
- Detecting inefficient polling patterns
- Tuning `result_page_fetch_pause_millis` parameter

---

## Type System Extensions

The driver supports all Flink SQL types with automatic parameter interpolation and result decoding. Some types require driver-specific Python types.

### Special Types

**`SqlNone` - Typed NULL values**

Flink requires NULL values to have explicit types. Use `SqlNone` to specify NULL values with a Flink type:

```python
from confluent_sql import SqlNone

cursor.execute(
    "INSERT INTO users (id, name, age) VALUES (%s, %s, %s)",
    (
        1,
        "Alice",
        SqlNone.INTEGER  # NULL with INTEGER type
    )
)

# For non-scalar types, pass the Flink type string
null_array = SqlNone("Array<int>")
cursor.execute("INSERT INTO data_column VALUES (%s)", (null_array,))
```

**`YearMonthInterval` - Year-month intervals**

Flink's `INTERVAL YEAR TO MONTH` type requires the driver's `YearMonthInterval` dataclass:

```python
from confluent_sql import YearMonthInterval
from datetime import timedelta

interval = YearMonthInterval(years=1, months=6)  # 1 year, 6 months
cursor.execute("SELECT * FROM orders WHERE created > (NOW() - %s)", (interval,))
```

### Complete Type Reference

For comprehensive type support documentation, see **[TYPES.md](TYPES.md)**:

- **Complete type mapping table** - Flink types ↔ Python types
- **Parameter interpolation** - Encoding Python values to Flink SQL
- **Result decoding** - Decoding Flink values to Python types
- **Type conversion caveats** - Edge cases and limitations
- **ARRAY and MAP examples** - Complex nested types
- **ROW type registration** - Custom type mapping
- **Examples from tests** - Real, tested code samples

---

## Custom Exceptions

### Standard DB-API Exceptions

All standard DB-API v2 exceptions are available:

- `Warning` - Warning messages
- `Error` - Base exception class
- `InterfaceError` - Interface-related errors
- `DatabaseError` - Database-related errors
- `DataError` - Data value-related errors
- `OperationalError` - Database operation errors
- `IntegrityError` - Relational integrity errors
- `InternalError` - Internal database errors
- `ProgrammingError` - Programming/SQL errors
- `NotSupportedError` - Unsupported feature

### Streaming-Specific Exceptions

Additional exceptions for streaming query lifecycle:

| Exception                   | Inherits From           | When Raised                                             |
| --------------------------- | ----------------------- | ------------------------------------------------------- |
| `StatementStoppedError`     | `OperationalError`      | Streaming statement stops unexpectedly during iteration |
| `StatementDeletedError`     | `StatementStoppedError` | Statement was deleted (404 from server)                 |
| `ComputePoolExhaustedError` | `OperationalError`      | Compute pool has no available resources                 |

**`StatementStoppedError` Attributes:**

- `statement_name` - Name of the stopped statement
- `statement` - Statement object (if available)
- `phase` - Terminal phase (STOPPED, FAILED, etc.)

**Exception Handling Example:**

```python
from confluent_sql import StatementStoppedError, ComputePoolExhaustedError
import time

cursor = connection.streaming_cursor()

try:
    cursor.execute("SELECT * FROM streaming_orders")

    while cursor.may_have_results:
        try:
            row = cursor.fetchone()
            if row:
                process(row)
            else:
                time.sleep(0.1)
        except ComputePoolExhaustedError:
            print("Compute pool exhausted, backing off")
            time.sleep(30)
except StatementStoppedError as e:
    print(f"Query stopped: {e.statement_name}")
    print(f"Final phase: {e.phase}")
```

---

## Extended Execute Parameters

The `cursor.execute()` method accepts additional parameters beyond standard DB-API for controlling statement execution and management.

### Signature

```python
cursor.execute(
    statement_text: str,
    parameters: tuple | list | None = None,
    *,
    timeout: int = 3000,
    statement_name: str | None = None,
    statement_label: str | None = None,
) -> None
```

### Parameter Reference

| Parameter         | Type                    | Default    | Description                                                        |
| ----------------- | ----------------------- | ---------- | ------------------------------------------------------------------ |
| `statement_text`  | `str`                   | (required) | SQL statement to execute                                           |
| `parameters`      | `tuple \| list \| None` | None       | Parameter values for parameterized statements                      |
| `timeout`         | `int`                   | 3000       | Max seconds to wait for statement to reach RUNNING/COMPLETED phase |
| `statement_name`  | `str \| None`           | None       | Custom statement identifier (defaults to UUID)                     |
| `statement_label` | `str \| None`           | None       | Label for grouping related statements                              |

**Examples:**

```python
# Basic execution
cursor.execute("SELECT * FROM users")

# With parameters
cursor.execute("SELECT * FROM users WHERE age > %s", (18,))

# With custom timeout
cursor.execute(
    "SELECT * FROM users",
    timeout=5000  # Wait up to 5000 seconds
)

# With statement naming
cursor.execute(
    "SELECT * FROM orders WHERE status = %s",
    ("completed",),
    statement_name="completed-orders-daily"
)

# With statement labeling (for batch operations)
cursor.execute(
    "CREATE TABLE orders_backup AS SELECT * FROM orders",
    statement_label="daily-backups"
)

# All together
cursor.execute(
    "SELECT product_id, COUNT(*) as sales FROM orders GROUP BY product_id",
    (),
    timeout=3000,
    statement_name="product-sales-hourly",
    statement_label="analytics"
)
```

---

## See Also

- [README.md](README.md) - Quick start and overview
- [STREAMING.md](STREAMING.md) - Comprehensive streaming query guide
- [TYPES.md](TYPES.md) - Complete type support reference
- [ARCHITECTURE.md](ARCHITECTURE.md) - HTTP architecture and statement lifecycle
