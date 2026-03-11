# Architecture

The `confluent-sql` driver is built on top of Confluent Cloud's HTTP-based Flink SQL API. Understanding the architecture helps explain key design decisions and enables advanced usage patterns.

## HTTP-Based Design

The driver communicates with Confluent Cloud Flink SQL through REST API endpoints. This architecture has several implications:

**How it works:**

1. When you call `cursor.execute()` or `connection.execute_snapshot_ddl()`, the driver sends an HTTP POST request to create a statement on the server
2. The server returns a unique statement ID and initial metadata
3. The driver then polls the server (HTTP GET requests) to fetch results and check statement status
4. Results are fetched in pages, allowing control over memory usage and backpressure

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

| Phase         | Meaning                                      | Can Fetch Results? | Typical Duration           |
| ------------- | -------------------------------------------- | ------------------ | -------------------------- |
| **PENDING**   | Queued on server, not yet started            | No                 | Usually < 100ms            |
| **RUNNING**   | Actively executing                           | Yes (if unbounded) | Seconds to indefinite      |
| **COMPLETED** | Finished successfully                        | Yes                | Indefinite (until deleted) |
| **FAILED**    | Error during execution                       | No                 | Indefinite (until deleted) |
| **STOPPED**   | User requested stop via `delete_statement()` | No                 | Indefinite (until deleted) |

### How Phases Progress

**Snapshot queries (the default):**

```
PENDING (< 100ms) → RUNNING → COMPLETED (seconds)
```

When you call `cursor.execute()` for a snapshot query, the driver:

1. Submits the statement (HTTP POST)
2. Polls the server until it reaches COMPLETED phase
3. Fetches the entire result set
4. Returns results to your code

This is blocking—your code pauses until the server finishes.

**Streaming queries:**

```
PENDING (< 100ms) → RUNNING (indefinite)
```

When you use `connection.streaming_cursor()`, the statement stays in RUNNING phase. Your code polls for results:

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
        time.sleep(0.1)  # Brief pause, check again soon
```

**DDL statements (CREATE TABLE, etc.):**

```
PENDING (< 100ms) → RUNNING → COMPLETED (usually instant)
```

DDL executes and completes quickly. Note: `execute_snapshot_ddl()` waits for COMPLETED, while `execute_streaming_ddl()` returns as soon as the job starts (RUNNING phase for continuous jobs).

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
# Group multiple statements with a label
cursor1 = connection.cursor()
cursor1.execute(
    "SELECT COUNT(*) FROM events",
    statement_label="hourly-metrics"
)

cursor2 = connection.cursor()
cursor2.execute(
    "SELECT * FROM users",
    statement_label="hourly-metrics"
)

# Later, find all statements with that label
statements = connection.list_statements(label="hourly-metrics")
for stmt in statements:
    print(f"{stmt.name}: {stmt.phase}")

# Delete all statements with that label
connection.delete_statement(label="hourly-metrics")
```

Labels allow you to organize statements logically without requiring unique names per statement—multiple statements can share the same label.

### Statement Persistence and Recovery

Statements persist on the server independently of your client connection, but are **scoped to the compute pool** where they were submitted. Statements can only be found, monitored, and recovered within the same compute pool:

1. **Create a statement with a name** for recovery:

   ```python
   statement = connection.execute_snapshot_ddl(
       "CREATE TABLE daily_summary AS SELECT * FROM events WHERE date > %s",
       (start_date,),
       statement_name="daily-summary-job"
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

### Statement Result Retention and Lifetime

**Result Availability:**

- Results are only retained server-side **while you are actively fetching them**
- Once you have fetched all results from a statement via `fetchone()`, `fetchmany()`, or `fetchall()`, the server **does not retain those results**
- If you need to access results again, you must re-execute the query (submit a new statement)
- The statement results API acts like a single, forward-only cursor.

**Automatic Cleanup:**

- Statements that produce results (e.g., SELECT queries) will be automatically **STOPPED** by the server if results are not fetched within a reasonable amount of time (typically minutes)
- STOPPED statements are eventually garbage collected by the server after a generous retention period (~weeks), after which they become unrecoverable

**Best Practices:**

```python
# ✅ Good: Fetch results immediately after execute()
cursor = connection.cursor()
cursor.execute("SELECT * FROM large_table")
for row in cursor:  # Start fetching right away
    process(row)

# ❌ Problematic: Long delay between execute() and fetch
cursor = connection.cursor()
cursor.execute("SELECT * FROM large_table")
time.sleep(3600)  # ← If server timeout is < 1 hour, statement may be STOPPED
rows = cursor.fetchall()  # May fail—statement was auto-deleted
```

**For Streaming Queries:**

- Keep your polling loop active and responsive
- If you need to pause processing, use appropriate timeouts on `fetchmany()`—don't abandon the statement for extended periods

### Statement Properties and Metadata

Each statement includes metadata available via the `Statement` object:

**From cursor.statement after execute():**

```python
cursor = connection.streaming_cursor()
cursor.execute("SELECT customer_id, COUNT(*) FROM events GROUP BY customer_id")

stmt = cursor.statement
print(f"ID: {stmt.statement_id}")           # Unique server-side ID
print(f"Name: {stmt.name}")                 # Assigned name (if set)
print(f"Phase: {stmt.phase}")               # PENDING, RUNNING, COMPLETED, etc.
print(f"Bounded: {stmt.is_bounded}")        # False for streaming queries
print(f"Append-only: {stmt.is_append_only}")  # True if no UPDATE/DELETE
print(f"Returns changelog: {stmt.returns_changelog}")  # True if includes operations
```

**Key properties for different use cases:**

- `phase` - Determine statement status (running? completed? failed?)
- `is_bounded` - False for unbounded/streaming queries
- `is_append_only` - True if results contain only INSERTs (no state tracking needed)
- `returns_changelog` - True if results include UPDATE/DELETE operations (need compressor?)
- `created_at` - When the statement was submitted
- `metadata` - Server-provided timing and execution information

## Performance Implications

### Snapshot Mode

Snapshot queries are **blocking**—your code pauses until the server finishes:

```python
cursor = connection.cursor()
cursor.execute("SELECT * FROM large_table")  # ← Blocks here
# Only reaches this line after server completes query
rows = cursor.fetchall()
```

Network latency directly impacts execution time:

- POST (submit) ~50-200ms
- GET (check status) ~50-200ms × N times until COMPLETED
- GET (fetch results) ~50-200ms × N times for N result pages

**Good for:** Ad-hoc queries, analytics, ETL where you want to block until results are ready.

### Streaming Mode

Streaming queries are **non-blocking**—you control when to poll:

```python
cursor = connection.streaming_cursor()
cursor.execute("SELECT * FROM kafka_topic")  # ← Returns immediately
# Statement is now RUNNING on server, you poll when ready

while cursor.may_have_results:
    rows = cursor.fetchmany(10)  # Get 10 rows or fewer if not available
    if rows:
        process(rows)
    else:
        time.sleep(0.1)  # Can sleep, do other work, etc.
```

This gives you backpressure control and allows interleaving other work.

**Good for:** Applications that handle continuous data, dashboards, monitoring systems.

## Statement Creation Methods

### Via cursor.execute()

Most common—returns a `Statement` object via `cursor.statement`:

```python
cursor = connection.cursor()
cursor.execute("SELECT * FROM table WHERE id = %s", (123,))
print(cursor.statement.phase)  # Access statement after execute
```

### Via execute_snapshot_ddl()

For DDL statements that should block until completion:

```python
statement = connection.execute_snapshot_ddl(
    "CREATE TABLE my_table AS SELECT * FROM source",
    statement_name="create-my-table"
)
print(statement.phase)  # Usually COMPLETED
```

### Via execute_streaming_ddl()

For long-running statements (DDL, INSERT INTO ... SELECT pipelines):

```python
statement = connection.execute_streaming_ddl(
    """
    INSERT INTO target_table
    SELECT col1, col2 FROM source_table
    WHERE col3 > %s
    """,
    (threshold,),
    statement_name="hourly-sync"
)
print(statement.phase)  # RUNNING (job is running indefinitely)
```

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

1. **Familiarity** - Behaves like traditional SQL databases (PostgreSQL, MySQL, etc.)
2. **Simplicity** - No special streaming knowledge required for basic queries
3. **Blocking semantics** - Matches DB-API v2 expectations
4. **Safety** - Can't accidentally call `fetchall()` on unbounded data

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

1. Takes your SQL template and parameter tuple
2. Converts parameters to appropriate SQL literals
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
