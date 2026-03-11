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
- Statements are **persistent server-side entities**, not temporary request/response pairs

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

### Statement Persistence and Recovery

Statements persist on the server independently of your client connection. You can:

1. **Create a statement with a name** for recovery:

   ```python
   statement = connection.execute_snapshot_ddl(
       "CREATE TABLE daily_summary AS SELECT * FROM events WHERE date > %s",
       (start_date,),
       statement_name="daily-summary-job"
   )
   ```

2. **Recover it from another connection:**
   ```python
   # In a different Python process or session
   statements = connection.list_statements(label="daily-jobs")
   for stmt in statements:
       if stmt.name == "daily-summary-job":
           print(f"Status: {stmt.phase}")  # COMPLETED, RUNNING, etc.
           # Can delete when done: connection.delete_statement(stmt.name)
   ```

This enables patterns like:

- **Job monitoring** - Check status of long-running queries from a different process
- **Graceful shutdown** - Find and delete statements before closing your application
- **Batch management** - Group related statements with labels for group operations
- **Job recovery** - Resume work without resubmitting if the client crashes

### Important: Statement Result Retention and Lifetime

Understanding statement lifetime is critical for production applications:

**Result Availability:**

- Results are only retained server-side **while you are actively fetching them**
- Once you have fetched all results from a statement via `fetchone()`, `fetchmany()`, or `fetchall()`, the server **does not retain those results**
- If you need to access results again, you must re-execute the query (submit a new statement)

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

## See Also

- [README.md](README.md) - Quick start and installation
- [DB-API Extensions](DBAPI_EXTENSIONS.md) - Practical APIs for statement management
- [Streaming Queries](STREAMING.md) - In-depth streaming query patterns
- [Type Support](TYPES.md) - Flink type system reference
- [Confluent Cloud Flink SQL Docs](https://docs.confluent.io/cloud/current/flink/) - Official Confluent documentation
