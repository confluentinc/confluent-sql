# Architecture

The `confluent-sql` driver is built on top of Confluent Cloud's HTTP-based Flink SQL API. Understanding the architecture of
both Confluent Cloud Flink and this driver helps explain key design decisions and enables advanced usage patterns.

## HTTP-Based Design

The driver communicates with Confluent Cloud Flink SQL through REST API endpoints. This architecture has several implications:

**How it works:**

1. When you call `cursor.execute*()`, the driver sends an HTTP POST request to create a statement on the server
2. The server returns a unique statement ID and initial metadata
3. The driver then polls the server (HTTP GET requests) to fetch both the statement state and/or its results
4. Results are fetched in pages, internally buffering, allowing control over memory usage and backpressure

**Key implications:**

- Each logical operation (execute, fetch, delete) involves one or more HTTP requests
- Network latency affects execution timing—snapshot queries block until the server returns results
- Statements are **semi-persistent server-side entities**, not temporary request/response pairs

**API Documentation:**

For technical details on the underlying HTTP endpoints, see the Confluent Cloud API Reference:

- [Statements API](<https://docs.confluent.io/cloud/current/api.html#tag/Statements-(sqlv1)>) - Submit and manage statements
- [Statement Results API](<https://docs.confluent.io/cloud/current/api.html#tag/Statement-Results-(sqlv1)>) - Fetch query results

## Statement Lifecycle

When you submit a statement (query, DDL, or data ingestion job), it becomes a first-class entity on the server with its own lifecycle. This is unlike traditional databases where queries are ephemeral—in Confluent's API, **statements persist and can be recovered by name**.

### Lifecycle Phases

Every statement progresses through these phases:

| Phase         | Meaning                           | Can Fetch Results Through This Driver?               |
| ------------- | --------------------------------- | ---------------------------------------------------- |
| **PENDING**   | Queued on server, not yet started | No                                                   |
| **RUNNING**   | Actively executing                | No if snapshot query/cursor, Yes if streaming cursor |
| **COMPLETED** | Finished successfully             | Yes (for both query/cursor types)                    |
| **FAILED**    | Error during execution            | No                                                   |
| **STOPPED**   | Explicitly stopped through API.   | No                                                   |

### How Phases Progress

**Snapshot queries (the default):** _(⚠️ [Early Access](https://docs.confluent.io/cloud/current/flink/concepts/snapshot-queries.html))_

```
PENDING  → RUNNING → COMPLETED
```

When you call `cursor.execute()` for a snapshot query / default cursor, the driver:

1. Submits the statement (HTTP POST)
2. Polls the server until it reaches COMPLETED phase
3. Returns from `.execute()` to let you begin to interact with the cursor / results.

This is blocking—your code pauses until the server finishes statement execution and result set calculation.

_NOTE_: This behavior may change in future releases, moving to have `.execute()` return once RUNNING phase is entered,
moving the additional blocking to the fetching results interactions.

**Streaming queries:**

When you use `connection.streaming_cursor()`, the statement will stay in `RUNNING` phase indefinitely.
Your code polls for results:

```python
cursor = connection.streaming_cursor()
cursor.execute("SELECT * FROM kafka_topic WHERE price > %s", (100,))

# Statement is now RUNNING on the server
# You poll for results when ready
while cursor.may_have_results:
    rows = cursor.fetchmany(10)
    if rows:
        process(rows)
    else:
        # Brief pause, check again soon.
        # Sleep time based on your knowledge of the nature of the query and its source topics.
        time.sleep(0.5)
```

See the [Streaming Mode](#streaming-mode) section for more details on controlling .

**DDL statements (CREATE TABLE, etc.):**

DDL executes and completes quickly. Note: `execute_snapshot_ddl()` waits for COMPLETED, while `execute_streaming_ddl()` returns as soon as the job starts (RUNNING phase for continuous jobs, such as streaming `CREATE TABLE AS SELECT` or streaming `INSERT AS SELECT` statements).

### Statement Names and Identification

**Every statement automatically receives a name** when submitted to the server. This happens whether you explicitly set one or not:

```python
# Without explicit name: server generates one automatically
cursor = connection.cursor()
cursor.execute("SELECT * FROM users")
print(cursor.statement.name)  # e.g., "stmt-a1b2c3d4e5f6" (auto-generated)

# With explicit name: you control the name
statement = connection.execute_snapshot_ddl(
    "CREATE TABLE summary AS SELECT * FROM events",
    statement_name="create-summary-table"
)
print(statement.name)  # "create-summary-table"
```

**Name Uniqueness:**

Statement names **must be unique within the compute pool** they are submitted to. If you attempt to submit a statement with a name that already exists in the same compute pool, the server will reject it with an error. This means:

- Auto-generated names are guaranteed unique per compute pool
- If using explicit names, you must ensure they don't conflict with existing statements
- After deleting a statement, its name becomes available for reuse
- Names are scoped to the compute pool, not globally—different compute pools can have statements with the same name

```python
# This will work: same name in different compute pools
pool_a_cursor = connection_pool_a.cursor()
pool_a_cursor.execute(
    "SELECT * FROM data",
    statement_name="my-query"  # OK in pool A
)

pool_b_cursor = connection_pool_b.cursor()
pool_b_cursor.execute(
    "SELECT * FROM data",
    statement_name="my-query"  # OK in pool B (different pool)
)

# This will fail: same name in the same compute pool
cursor1 = connection.cursor()
cursor1.execute("SELECT * FROM table1", statement_name="my-query")

cursor2 = connection.cursor()
cursor2.execute("SELECT * FROM table2", statement_name="my-query")  # ❌ Error: name already exists
```

**Grouping Statements with Labels:**

In addition to individual statement names, you can use statement labels to group related statements together for batch operations:

```python
# Group multiple statements with labels
cursor1 = connection.cursor()
cursor1.execute(
    "SELECT COUNT(*) FROM events",
    statement_labels=["hourly-metrics", "analytics"]
)

cursor2 = connection.cursor()
cursor2.execute(
    "SELECT * FROM users",
    statement_labels=["hourly-metrics", "analytics"]
)

# Later, find all statements with any of the labels to then delete
statements = connection.list_statements(label="hourly-metrics")
for stmt in statements:
    print(f"{stmt.name}: {stmt.phase}")
    connection.delete_statement(stmt)
```

Labels allow you to organize statements logically without requiring unique names per statement—multiple statements can share the same label.

### Statement Persistence and Recovery

Statements persist on the server independently of your client connection, but are **scoped to the compute pool** where they were submitted. Statements can only be found, monitored, and recovered within the same compute pool:

1. **Create a statement with a name** for recovery:

   ```python
   statement = connection.execute_snapshot_ddl(
       "CREATE TABLE daily_summary AS SELECT * FROM events WHERE date > %s",
       (start_date,),
       statement_name="daily-summary-job",
       statement_labels=["daily-jobs", "etl"]
   )
   ```

2. **Recover it from another connection in the same compute pool:**
   ```python
   # In a different Python process or session (same compute pool)
   statements = connection.list_statements(label="daily-jobs")
   for stmt in statements:
       if stmt.name == "daily-summary-job":
           print(f"Status: {stmt.phase}")  # COMPLETED, RUNNING, etc.
           # Can delete when done: connection.delete_statement(stmt.name)
   ```

### Foreground vs Background Statements

Confluent Cloud Flink differentiates between 'foreground' and 'background' statements.

A 'background' statement is one where the results are being published to a Kafka topic/Flink table, such as an INSERT INTO or CREATE TABLE AS SELECT. Foreground statements are those whose result sets are returned to the client that submitted the statement, such as regular SELECT statements.

### Statement Result Retention and Lifetime

**Result Availability:**

- Results for foreground staetments are only retained server-side **while you are actively fetching them**
- Once you have fetched a page of results from a statement via `fetchone()`, `fetchmany()`, `fetchall()`, or cursor iteration, the server **does not retain those results**
- If you need to access results again, you must re-execute the query (submit a new statement)
- The statement results API acts like a single, forward-only cursor.

**Automatic Cleanup:**

- Foreground statements will be automatically **STOPPED** by the server if results are not fetched within a reasonable amount of time (typically minutes)
- STOPPED foreground statements are eventually garbage collected by the server after a generous retention period (~weeks).

### Statement Properties and Metadata

Each statement includes metadata available via the `Statement` object:

**From cursor.statement after execute():**

```python
cursor = connection.streaming_cursor()
cursor.execute("SELECT customer_id, COUNT(*) FROM events GROUP BY customer_id")

stmt = cursor.statement
print(f"ID: {stmt.statement_id}")             # Unique server-side ID
print(f"Name: {stmt.name}")                   # Assigned statement name
print(f"Phase: {stmt.phase}")                 # PENDING, RUNNING, COMPLETED, etc.
print(f"Bounded: {stmt.is_bounded}")          # False for streaming queries, true for those done in 'snapshot' mode.
print(f"Append-only: {stmt.is_append_only}")  # True if statement results changelog is simple -- no UPDATE/DELETE operations
```

## Performance Implications

### Snapshot Mode

Snapshot queries are **blocking**—your code pauses until the server finishes computing the result set:

```python
cursor = connection.cursor()
cursor.execute("SELECT * FROM large_table")  # ← Blocks here until the statement reaches `COMPLETED` phase.
rows = cursor.fetchall()
```

**Good for:** Ad-hoc queries, analytics, ETL where you want to block until results are ready.

### Streaming Mode

With streaming queries, you choose between **blocking** and **non-blocking** interaction patterns:

#### Non-Blocking: Using `fetch*()` Methods

The `fetchone()`, `fetchmany()`, and `fetchall()` methods return immediately without waiting, even if no rows are available. You manage your own polling loop:

```python
cursor = connection.streaming_cursor()
cursor.execute("SELECT * FROM kafka_topic")  # ← Returns when statement enters RUNNING phase
# Statement is now RUNNING on server, you poll when ready

while cursor.may_have_results:
    rows = cursor.fetchmany(10)  # Returns immediately with 0-10 rows (possibly empty!)

    if rows:
        process(rows)
    else:
        # No rows available right now, can do other work or pause.
        # Pause time would best be based on knowledge of the query and the
        # event streams into the source topics the query draws from.
        time.sleep(1)
```

This gives you backpressure control and allows interleaving other work.

**Good for:** Applications that handle continuous data with backpressure control, dashboards, monitoring systems, or anything doing other work concurrently with polling.

#### Blocking: Using Iteration

Iterating over the cursor with `for row in cursor:` uses a **blocking** pattern. The cursor internally fetches pages of results and yields rows one-by-one from its buffer, blocking indefinitely when the buffer is empty and waiting for the server to produce new results:

```python
cursor = connection.streaming_cursor()
cursor.execute("SELECT * FROM kafka_topic")
# Statement is now RUNNING on server

for row in cursor:
    # Yields rows from internal buffer; blocks when buffer is empty
    # waiting for server to produce more rows
    process(row)
```

This simplifies your code by eliminating manual polling loops, but your application blocks whenever the server hasn't produced new rows yet.

**Good for:** Simple, focused streaming applications where straightforward row-by-row processing is the priority and blocking while waiting for data is acceptable.

### Finding Existing Statements

List statements by label (useful for batch operations):

```python
# Find all statements with a specific label
statements = connection.list_statements(label="daily-jobs", page_size=100)
for stmt in statements:
    print(f"{stmt.name}: {stmt.phase}")
    if stmt.phase == "FAILED":
        print(f"  Error: {stmt.status.get('error')}")
```

## Design Trade-Offs

### Why Snapshot Mode Is Default

1. **Blocking + Finite Result Set semantics** - Matches DB-API v2 expectations
2. **Familiarity** - Behaves like traditional SQL databases
3. **Simplicity** - No special streaming knowledge required for basic queries

Streaming mode is opt-in (via `streaming_cursor()`) for applications that explicitly need continuous data.

## Caveats

### Parameter Handling and String Interpolation

The Confluent Cloud Flink SQL HTTP API does not natively support supplying query parameters separately from the statement SQL text. As a result, the `confluent-sql` driver is responsible for performing client-side parameter interpolation.

**How it works:**

When you use parameterized queries with `%s` placeholders:

```python
cursor.execute(
    "SELECT * FROM users WHERE age > %s AND status = %s",
    (18, 'active')
)
```

The driver:

1. Takes your SQL template and parameters tuple
2. Converts parameters to appropriate SQL literals based on the class of the parameter as documented in [TYPES.md](TYPES.md)
3. Interpolates them into the SQL string
4. Submits the complete SQL to the server

The resulting SQL sent to Flink would be:

```sql
SELECT * FROM users WHERE age > 18 AND status = 'active'
```

**String Escaping:**

Every effort is made to properly escape string parameters to prevent SQL injection:

- Single quotes in string values are escaped (e.g., `'it''s'` for the string `it's`)
- String values are wrapped in single quotes
- Complex types listed in [TYPES.md](TYPES.md) are converted to their text representation properly.

**Limitations and Best Practices:**

1. **Client-side interpolation only** - Because Flink's API doesn't support server-side parameter binding, the driver cannot use prepared statements or parameterized queries at the server level
2. **String escaping is defensive** - While string escaping is carefully implemented, avoid constructing queries with user input directly when possible
3. **Use parameterized queries for safety** - Always use `%s` placeholders with the parameter tuple, never concatenate user input directly into SQL strings:

   ```python
   # ✅ Good: Use parameterized queries
   cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))

   # ❌ Avoid: String concatenation (vulnerable to injection)
   cursor.execute(f"SELECT * FROM users WHERE id = {user_id}")
   ```

4. **Special SQL values** - If you need to use SQL keywords or identifiers, you may need to quote them in the SQL template, not as parameters:

   ```python
   # Parameterize values, but use quoted identifiers for column names
   cursor.execute(
       'SELECT "user_id", "email" FROM users WHERE age > %s',
       (18,)
   )
   ```

This design reflects the current capabilities of the Confluent Cloud Flink SQL API and prioritizes security through proper escaping while providing a familiar DB-API v2 interface.

## See Also

- [README.md](README.md) - Quick start and installation
- [DB-API Extensions](DBAPI_EXTENSIONS.md) - Practical APIs for statement management
- [Streaming Queries](STREAMING.md) - In-depth streaming query patterns
- [Type Support](TYPES.md) - Flink type system reference
- [Confluent Cloud Flink SQL Docs](https://docs.confluent.io/cloud/current/flink/) - Official Confluent documentation
