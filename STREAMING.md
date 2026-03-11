# Streaming Queries

The `confluent-sql` driver provides full support for continuous streaming queries with Confluent Cloud Flink SQL. This guide covers how to execute streaming queries, handle different result types, and leverage the changelog compressor for stateful query results.

## Overview

This driver supports two primary execution modes:

| Mode                   | Use Case              | Query Examples                  | Result Behavior                                                              |
| ---------------------- | --------------------- | ------------------------------- | ---------------------------------------------------------------------------- |
| **SNAPSHOT** (default) | Point-in-time results | Bounded queries, single state   | Returns one result set; blocking `fetchall()`                                |
| **STREAMING_QUERY**    | Continuous data       | Unbounded queries, time windows | Returns results as they arrive; cannot use `fetchall()` on unbounded queries |

## Creating Streaming Cursors

### Method 1: Using `closing_streaming_cursor()` (Recommended)

For streaming cursors with automatic resource cleanup via context manager:

```python
with connection.closing_streaming_cursor(as_dict=True) as cursor:
    cursor.execute("SELECT * FROM orders_stream WHERE total > %s", (1000,))
    while cursor.may_have_results:
        rows = cursor.fetchmany(10)
        if rows:
            for row in rows:
                print(row)
# cursor automatically closed
```

This is the **preferred pattern for streaming queries** because it:

- ✅ Automatically closes the cursor on context exit (client-side cleanup)
- ✅ Works correctly even if exceptions occur
- ✅ Makes intent clear: "I'm creating a streaming cursor with auto-cleanup"
- ✅ No need to import `ExecutionMode`
- ℹ️ For long-running streaming jobs that remain RUNNING on the server, explicitly stop them with `delete_statement()` before exiting the context manager to avoid resource leaks

See [DBAPI_EXTENSIONS.md](DBAPI_EXTENSIONS.md#auto-closing-streaming-cursor-closing_streaming_cursor) for more details on context managers.

### Method 2: Using `streaming_cursor()`

The simplest way to create a streaming cursor without context manager cleanup:

```python
cursor = connection.streaming_cursor()
```

Note: You can also use the optional `as_dict=True` parameter to return rows as dictionaries with column names as keys instead of tuples (available for both snapshot and streaming queries).

### Method 3: Using `cursor()` with mode parameter

For more control over execution mode:

```python
from confluent_sql.execution_mode import ExecutionMode

cursor = connection.cursor(mode=ExecutionMode.STREAMING_QUERY)
```

## Cursor Properties for Streaming Queries

When working with streaming cursors, several properties help you understand and control query behavior:

| Property                   | Type   | Description                                                                                                                                           |
| -------------------------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cursor.may_have_results`  | `bool` | Check if more data may arrive later (vs permanently exhausted). Essential for polling loops—distinguishes temporary emptiness from stream completion. |
| `cursor.returns_changelog` | `bool` | Detect whether results include changelog operations (INSERT/UPDATE/DELETE) or are append-only. Use to determine if you need a changelog compressor.   |
| `cursor.is_streaming`      | `bool` | Convenience check for streaming mode (`execution_mode == ExecutionMode.STREAMING_QUERY`).                                                             |

**Example:**

```python
cursor = connection.streaming_cursor()
cursor.execute("SELECT product_id, COUNT(*) FROM orders GROUP BY product_id")

# Check query characteristics
if cursor.returns_changelog:
    # Results are ChangeloggedRow with operations
    compressor = cursor.changelog_compressor()
    for snapshot in compressor.snapshots():
        process(snapshot)
else:
    # Results are plain rows (append-only)
    while cursor.may_have_results:
        rows = cursor.fetchmany(10)
        if rows:
            process(rows)
        else:
            time.sleep(0.1)
```

## Append-Only Streaming Queries

### What Are Append-Only Streaming Queries?

Append-only queries produce only INSERT operations—no updates or deletes. They result from:

- Simple streaming-mode SELECT with WHERE clause
- Filtering and projection of streaming data
- No aggregations, joins, or windows

**Example queries:**

```sql
SELECT * FROM kafka_topic WHERE price > 100
SELECT order_id, customer_id, amount FROM orders
```

### Identifying Append-Only Queries

```python
cursor = connection.streaming_cursor()
cursor.execute("SELECT * FROM kafka_topic WHERE price > %s", (100,))

# After execute, check if query is append-only. Append-only streaming
# statements can be handled through the cursor interface directly, albeit
# not with .fetchall()!
if cursor.statement.is_append_only:
    print("Results contain only inserts (plain rows)")
else:
    print("Results contain updates/deletes (changelog)")
```

### Polling for Results

#### Pattern 1: Single Row Polling

```python
import time

cursor = connection.streaming_cursor(as_dict=True)
cursor.execute("SELECT * FROM orders WHERE total > %s", (1000,))

while cursor.may_have_results:
    # Will internally gather a page of results at a time and internally buffer.
    row = cursor.fetchone()
    if row is not None:
        print(f"Order: {row}")
    else:
        # No data currently available, but more may arrive
        time.sleep(0.5)
```

#### Pattern 2: Batch Polling

```python
import time

cursor = connection.streaming_cursor(as_dict=True)
cursor.execute("SELECT * FROM orders WHERE total > %s", (1000,))

while cursor.may_have_results:
    # Possibly fetch a new page of results, returning at most
    # 10 rows at a time.
    rows = cursor.fetchmany(10)
    if rows:
        for row in rows:
            print(f"Order: {row}")
    else:
        time.sleep(0.5)
```

#### Pattern 3: Blocking Iteration

```python
# Blocks until data arrives, suitable for continuous consumption
cursor = connection.streaming_cursor(as_dict=True)
cursor.execute("SELECT * FROM orders WHERE total > %s", (1000,))

for row in cursor:
    print(f"Order: {row}")
    # Iterates continuously until statement stops
```

---

## Changelog Streaming Queries

### What Are Changelog Queries?

Changelog queries include stateful operations that can modify previous results:

- Aggregations: `GROUP BY`, `COUNT()`, `SUM()`, etc.
- Joins: `JOIN`, `CROSS JOIN`
- Windows: `TUMBLE()`, `HOP()`, etc.

**Example queries:**

```sql
SELECT
    user_id, COUNT(*) as order_count
FROM orders GROUP BY user_id


SELECT a.id, a.amount, b.status
FROM
    orders a
    JOIN shipments b ON a.id = b.order_id
```

## Changelog Compressor (Recommended High-Level API)

The **recommended** way to handle changelog queries is using the `ChangelogCompressor`, which automatically maintains the current state by applying changelog operations. This high-level API handles all the complexity of processing changelog events for you.

### Why Use the Changelog Compressor?

Without a compressor, you must manually:

1. Match UPDATE_BEFORE/UPDATE_AFTER pairs
2. Track INSERT/DELETE operations
3. Maintain a logical result set

The compressor handles all of this automatically:

```python
# ❌ Manual changelog processing (complex, error-prone)
for row in cursor:
    op, data = row
    if op == Op.UPDATE_BEFORE:
        # Store this...
    elif op == Op.UPDATE_AFTER:
        # Match with previous UPDATE_BEFORE...
        # Update logical state...

# ✅ Using compressor (simple, correct)
compressor = cursor.changelog_compressor()
for snapshot in compressor.snapshots():
    # the 'snapshot' is the currently self-consistent
    # aggregated state of the result set tuples or dicts.
    for row in snapshot:
        process(row)
```

### Determining When to Use the Changelog Compressor

After executing a streaming query, use `cursor.returns_changelog` to determine which API to use for processing results:

```python
from datetime import datetime

cursor = connection.streaming_cursor()
cursor.execute("""
    SELECT product_id, COUNT(*) as sales FROM orders
    WHERE order_date > %s
    GROUP BY product_id
""", (datetime(2024, 1, 1),))

if cursor.returns_changelog:
    # Results include INSERT, UPDATE_BEFORE/AFTER, and DELETE operations
    # Use the changelog compressor for automatic state management
    compressor = cursor.changelog_compressor()
    for snapshot in compressor.snapshots():
        for row in snapshot:  # Iterate over rows in the snapshot
            process(row)  # Current row from aggregated state
else:
    # Results are append-only (only INSERT operations)
    # Use the cursor interface directly
    for row in cursor:
        process(row)  # Plain row data
```

This flexible approach allows you to write generic streaming code that automatically adapts to the query type: stateful queries with the high-level compressor API, or simple append-only queries with the standard cursor interface.

### Creating a Changelog Compressor

```python
from datetime import datetime

cursor = connection.streaming_cursor(as_dict=True)
cursor.execute("""
    SELECT product_id, COUNT(*) as sales FROM orders
    WHERE order_date > %s
    GROUP BY product_id
""", (datetime(2024, 1, 1),))

# Create compressor - type is automatically selected based on upsert columns
compressor = cursor.changelog_compressor()
```

### Pattern 1: Using `snapshots()` Generator

The `snapshots()` method yields a sequence of snapshots, automatically consuming all available changelog events between yields:

```python
import time
from datetime import datetime

cursor = connection.streaming_cursor(as_dict=True)
cursor.execute("""
    SELECT c1 % %s as parity, COUNT(*) as cnt FROM table
    WHERE event_time > %s
    GROUP BY c1 % %s
""", (2, datetime(2024, 1, 1), 2))

compressor = cursor.changelog_compressor()

# Iterate over snapshots until the query stops
for snapshot in compressor.snapshots():
    # Each snapshot is a list of current rows reflecting all operations seen so far
    print(f"Current state: {snapshot}")
    time.sleep(5)  # Optional: wait before fetching next snapshot

# Generator automatically raises StatementStoppedError when query ends
```

**Use `snapshots()` when:**

- You want automatic polling
- You want automatic termination detection
- You don't need custom retry logic
- You prefer exception-based termination (Pythonic for infinite streams)

**Key behavior:**

- Blocks waiting for data if necessary
- Returns deep copy of internal state
- No guarantee consecutive snapshots differ
- Raises `StatementStoppedError` when query stops

### Pattern 2: Using `get_current_snapshot()` for Custom Loops

The `get_current_snapshot()` method fetches available events and returns a single snapshot, giving you control over the polling loop:

```python
import time
from datetime import datetime

cursor = connection.streaming_cursor(as_dict=True)
cursor.execute("""
    SELECT product_id, COUNT(*) as sales FROM orders
    WHERE order_date > %s
    GROUP BY product_id
""", (datetime(2024, 1, 1),))

compressor = cursor.changelog_compressor()

# You control the loop - responsibility to check may_have_results
max_iterations = 100
for iteration in range(max_iterations):
    if not cursor.may_have_results:
        break

    # Fetch current snapshot
    snapshot = compressor.get_current_snapshot()
    print(f"Iteration {iteration}: {snapshot}")

    # Your custom wait logic
    time.sleep(1)
```

**Use `get_current_snapshot()` when:**

- You need custom loop control
- You want specific retry/backoff logic
- You're integrating with async frameworks
- You need to check other conditions in the loop
- You prefer explicit loop control over generators

**Key behavior:**

- Non-blocking - returns immediately after consuming available events
- Returns deep copy of internal state
- Caller must check `cursor.may_have_results`
- Does not raise `StatementStoppedError`

## Understanding Snapshots of Results from Changelog Compressor

When you use a changelog compressor to process results from a streaming aggregation or join query, you work with **snapshots of results**—not individual changelog events. Each snapshot is a complete accumulated result set at a specific point in time.

### What is a Snapshot of Results?

A **snapshot of results** is a self-consistent, complete result set showing the accumulated state of your data at a specific point in time.

**Key Characteristics of a Snapshot of Results:**

- **Current State of All Rows**: Built from all INSERT/UPDATE/DELETE operations processed so far
- **Point-in-Time View**: Represents state after consuming all available changelog events at that moment
- **Self-Consistent**: No pending operations awaiting completion (all UPDATE_BEFORE/UPDATE_AFTER pairs have been applied)
- **Complete Result Set, Not Incremental**: Each snapshot is the full accumulated result set at this moment in time, not just the changes since the last snapshot

Don't think of snapshots of results as collections of individual changelog events. Instead, think of them as complete result sets:

```python
cursor = connection.streaming_cursor()
cursor.execute("SELECT product_id, SUM(amount) FROM orders GROUP BY product_id")
compressor = cursor.changelog_compressor()

for snapshot in compressor.snapshots():
    # Each snapshot of results contains the COMPLETE current totals by product
    # NOT just the rows that changed since the last snapshot
    # e.g., [(1, 1000), (2, 2500), (3, 500)]

    for product_id, total_amount in snapshot:
        update_dashboard(product_id, total_amount)

    time.sleep(5)  # Wait for more orders to arrive
```

In this example, each snapshot of results is a complete list of all products and their current totals—not a list of what changed.

### When Snapshots of Results Are Identical

If no new changelog events are fetchable from the results API between two polling intervals, consecutive snapshots of results will be identical. This is completely normal and expected. Your application can choose to:

- Process all rows within every snapshot regardless (simple approach)
- Use delta detection to understand what actually changed. If this is needed, it may be simpler to consume the changelog events directly from the cursor and not use the compressor API.

### How Snapshot Boundaries Are Inferred

The changelog compressor infers snapshot boundaries based on the statement's result route indicator. A **snapshot capstone** (the point marking the end of a snapshot) is inferred when the result route indicates "no more changelog rows available at this time." This signals that all pending changelog events have been processed and the current state is self-consistent.

**Important Limitation:** Flink SQL does not include explicit markers in the changelog stream that say "the changelog is self-consistent at this time." This means:

- **Explicit Boundaries Only**: The compressor can only reliably identify snapshot boundaries when polling returns no new rows
- **Interior Points Missed**: Self-consistent points in time that occur between changelog rows may not be captured as explicit snapshots
- **Partial Coverage**: The `snapshots()` interface provides some of the self-consistent result set points in time, but not necessarily all of them

In practice, this means your application will capture snapshots when you poll and receive no new changelog events, ensuring you have a reliable periodic view of the complete accumulated state. However, if additional self-consistent points occur between your polling intervals (within the incoming changelog stream), those intermediate points won't be explicitly available as separate snapshots.

**Polling Pattern Impact:**

The frequency and timing of your polling directly affects which self-consistent points you capture:

```python
# Frequent polling: More likely to capture intermediate self-consistent points
for snapshot in compressor.snapshots():
    time.sleep(0.1)  # Poll every 100ms

# Infrequent polling: May miss interior self-consistent points
for snapshot in compressor.snapshots():
    time.sleep(5)  # Poll every 5 seconds
```

Both approaches are valid—choose based on your application's latency and throughput requirements.

### Implementation

The changelog compressor retains a collection of the result set rows based on the sum of changelog events received since the start of statement execution. Each returned snapshot of those rows is a deepcopy of the compressor's internal state.

---

## Understanding Changelog Operations (Low-Level Details)

When executing a changelog query, results include operation metadata indicating how each row changed:

```python
from datetime import datetime

from confluent_sql import Op

cursor = connection.streaming_cursor()
cursor.execute("""
    SELECT user_id, COUNT(*) FROM orders
    WHERE order_date > %s
    GROUP BY user_id
""", (datetime(2024, 1, 1),))

# This is a changelog query (stateful aggregation with GROUP BY)
assert cursor.returns_changelog, "Expected changelog results from GROUP BY query"

row = cursor.fetchone()
# Result is a ChangeloggedRow with (op, row_data)
op, data = row

if op == Op.INSERT:
    print(f"New row: {data}")
elif op == Op.UPDATE_BEFORE:
    print(f"Previous value: {data}")
elif op == Op.UPDATE_AFTER:
    print(f"Updated value: {data}")
elif op == Op.DELETE:
    print(f"Deleted row: {data}")
```

### Changelog Operations

| Operation     | Code                   | Meaning                           |
| ------------- | ---------------------- | --------------------------------- |
| INSERT        | `Op.INSERT` (0)        | New row inserted (+I)             |
| UPDATE_BEFORE | `Op.UPDATE_BEFORE` (1) | Previous value before update (-U) |
| UPDATE_AFTER  | `Op.UPDATE_AFTER` (2)  | New value after update (+U)       |
| DELETE        | `Op.DELETE` (3)        | Row deleted (-D)                  |

### Detecting Changelog Results

```python
from datetime import datetime

cursor = connection.streaming_cursor()
cursor.execute("""
    SELECT user_id, COUNT(*) FROM orders
    WHERE order_date > %s
    GROUP BY user_id
""", (datetime(2024, 1, 1),))

# After execute, check if results will be changelog
if cursor.returns_changelog:
    print("Results are ChangeloggedRow(op, row) pairs")
else:
    print("Results are plain rows")
```

### Processing Changelog Events

```python
import time
from datetime import datetime

from confluent_sql import Op

cursor = connection.streaming_cursor(as_dict=True)
cursor.execute("""
    SELECT user_id, COUNT(*) as order_count FROM orders
    WHERE order_date > %s
    GROUP BY user_id
""", (datetime(2024, 1, 1),))

# Manually track updates
pending_updates = {}

while cursor.may_have_results:
    row = cursor.fetchone()
    if row:
        op, data = row

        if op == Op.INSERT:
            print(f"User {data['user_id']}: NEW with {data['order_count']} orders")
        elif op == Op.UPDATE_BEFORE:
            pending_updates[data['user_id']] = data['order_count']
        elif op == Op.UPDATE_AFTER:
            old_count = pending_updates.get(data['user_id'])
            print(f"User {data['user_id']}: {old_count} → {data['order_count']} orders")
        elif op == Op.DELETE:
            print(f"User {data['user_id']}: DELETED")
    else:
        time.sleep(0.1)
```

## Polling Patterns and Blocking Behavior

### Understanding `may_have_results`

The `may_have_results` property distinguishes between two conditions:

```python
while cursor.may_have_results:
    row = cursor.fetchone()
    if row is not None:
        process(row)
    else:
        # No data NOW, but may arrive LATER
        time.sleep(0.1)
```

- **`may_have_results == True`**: Stream is active; more data might arrive later
- **`may_have_results == False`**: Stream has ended; no more data will ever arrive

### When Does `may_have_results` Become False?

For **streaming queries**, `may_have_results` stays `True` as long as the statement is running and can produce results. It becomes `False` only when the statement reaches a **terminal phase** due to external stoppage:

- **`STOPPED`**: Query was manually stopped (external means)
- **`FAILED`**: Statement encountered an error during execution
- **`COMPLETED`**: Query finished naturally (usually only for snapshot mode execution)

```python
cursor = connection.streaming_cursor()
cursor.execute("SELECT * FROM orders_stream")

while cursor.may_have_results:
    rows = cursor.fetchmany(10)
    if rows:
        for row in rows:
            process(row)
    else:
        # No data available RIGHT NOW, but the streaming
        # query is still running and may_have_results is True
        time.sleep(0.1)  # Poll again later

# may_have_results becomes False when the query is stopped or fails
```

**Key Points:**

- `may_have_results == True` doesn't mean data is available now; it means the query is still active
- Empty fetch results during a `may_have_results == True` loop means "no data arrived in this poll" (not "stream ended")
- The query will remain `may_have_results == True` indefinitely (for unbounded streaming queries) until you explicitly stop it or it fails

### Snapshot vs Streaming Blocking Behavior

**Snapshot Mode (blocking):**

```python
cursor = connection.cursor(mode=ExecutionMode.SNAPSHOT)
cursor.execute("SELECT * FROM users WHERE active = %s", (True,))

# Blocks until a row is available (or all results exhausted)
row = cursor.fetchone()
```

**Streaming Mode (non-blocking):**

```python
cursor = connection.streaming_cursor()
cursor.execute("SELECT * FROM users_stream WHERE active = %s", (True,))

# Returns immediately - None if no data currently available
row = cursor.fetchone()  # Could be None even if more data arrives later
```

### Fetch Methods: Which to Use?

| Method              | Snapshot Mode | Streaming Mode      | Use Case                 |
| ------------------- | ------------- | ------------------- | ------------------------ |
| `fetchone()`        | Blocking      | Non-blocking        | Single row processing    |
| `fetchmany(size)`   | Blocking      | Non-blocking        | Batch processing         |
| `fetchall()`        | Blocking      | Raises if unbounded | Only for bounded queries |
| `for row in cursor` | Blocking      | Blocking            | Continuous consumption   |

## Complete Examples

### Example 1: Append-Only Stream with Dictionary Rows

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

    max_wait_seconds = 60
    start_time = time.time()

    while cursor.may_have_results:
        rows = cursor.fetchmany(10)
        if rows:
            for row in rows:
                print(f"Order {row['order_id']}: ${row['amount']} from customer {row['customer_id']}")
        else:
            if time.time() - start_time > max_wait_seconds:
                print("No new orders for 60 seconds, exiting")
                break
            time.sleep(1)
```

### Example 2: Changelog Stream with Compressor

```python
from datetime import datetime
import time

import confluent_sql

connection = confluent_sql.connect(...)

min_order_date = datetime(2024, 1, 1)
with connection.closing_streaming_cursor(as_dict=True) as cursor:
    cursor.execute("""
        SELECT product_id, product_name, COUNT(*) as sales_count
        FROM orders
        WHERE order_date > %s
        GROUP BY product_id, product_name
    """, (min_order_date,))

    compressor = cursor.changelog_compressor()

    iteration = 0
    for snapshot in compressor.snapshots():
        iteration += 1
        print(f"\n--- Snapshot {iteration} ---")

        # Sort by sales count descending
        sorted_snapshot = sorted(snapshot, key=lambda x: x['sales_count'], reverse=True)

        for row in sorted_snapshot[:5]:  # Top 5 products
            print(f"{row['product_name']}: {row['sales_count']} sales")

        time.sleep(5)
```

### Example 3: Custom Polling with `get_current_snapshot()`

```python
import time
from datetime import datetime

import confluent_sql

connection = confluent_sql.connect(...)

min_timestamp = datetime(2024, 1, 1)
with connection.closing_streaming_cursor(as_dict=True) as cursor:
    cursor.execute("""
        SELECT
            CAST(event_time / %s AS BIGINT) as hour,
            COUNT(*) as event_count,
            AVG(duration) as avg_duration
        FROM events
        WHERE event_time > %s
        GROUP BY CAST(event_time / %s AS BIGINT)
    """, (3600000, min_timestamp, 3600000))

    compressor = cursor.changelog_compressor()

    max_polls = 60  # Exit after 60 polls
    poll_count = 0
    last_snapshot_hash = None

    while cursor.may_have_results and poll_count < max_polls:
        snapshot = compressor.get_current_snapshot()

        # Check if state changed
        current_hash = hash(tuple(str(r) for r in snapshot))
        if current_hash != last_snapshot_hash:
            print(f"State changed - {len(snapshot)} hours with events")
            for row in snapshot:
                print(f"  Hour {row['hour']}: {row['event_count']} events, " +
                      f"avg duration {row['avg_duration']:.2f}ms")
            last_snapshot_hash = current_hash

        poll_count += 1
        time.sleep(2)  # Poll every 2 seconds

    print(f"Completed {poll_count} polls")
```

## Advanced Topics

### Handling Statement Lifecycle

```python
cursor = connection.streaming_cursor()
cursor.execute("SELECT * FROM stream WHERE active = %s", (True,), statement_label="my-streaming-job")

# Query statement information
print(f"Statement name: {cursor.statement.name}")
print(f"Phase: {cursor.statement.phase}")

# Check statement properties (see "Identifying Append-Only Queries" section)
if cursor.statement.is_append_only:
    print("Query produces append-only results")
else:
    print("Query produces changelog results")

# Later: delete the statement if needed
if cursor.statement.is_deletable:
    cursor.delete_statement()
```

### Monitoring Performance Metrics

```python
cursor = connection.streaming_cursor()
cursor.execute("SELECT * FROM orders WHERE active = %s", (True,))

# Access fetch metrics
metrics = cursor.metrics
print(f"Bytes fetched: {metrics.bytes_received}")
print(f"Rows fetched: {metrics.rows_returned}")
```

## See Also

- [DB-API Extensions](DBAPI_EXTENSIONS.md) - Comprehensive extension reference
- [Type Support](TYPES.md) - Comprehensive Flink type reference
- [README.md](README.md) - Quick start guide
- [Architecture & How It Works](ARCHITECTURE.md) - HTTP architecture and statement lifecycle
