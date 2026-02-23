# Automatic Memory Freeing via deque-Based Buffer Management

## TL;DR

**Problem**: Cursor retains all fetched rows in memory indefinitely, causing unbounded growth for large result sets.

**Solution**: Replace `list`-based buffer with `collections.deque` to enable automatic, incremental memory freeing as rows are consumed.

**Impact**: 99.9% memory reduction for large result sets (from O(total_rows) to O(page_size))

**Changes**:

- `changelog.py`: Replace list with deque, remove index tracking, **delete `clear_buffer()`**
- `cursor.py`: **Delete `clear_changelog_buffer()`** (redundant with automatic freeing)
- `changelog_compressor.py`: Remove redundant call
- ~40 lines modified, ~21 lines deleted across 3 files

**Risk**: Low (internal refactor, excellent test coverage, deque is stdlib and well-tested, no legacy API users)

---

## Problem Statement

Currently, the cursor and `ChangelogProcessor` retain all fetched rows in memory until either:

1. `close()` is called, or
2. `clear_buffer()` is explicitly invoked (only happens automatically in `ChangelogCompressor.snapshots()`)

This creates **unbounded memory growth** for common usage patterns:

### Current Memory Retention Issues

1. **Direct cursor iteration** (`for row in cursor:`):
   - All fetched rows accumulate in `_results` even after delivery to caller
   - Memory grows proportionally to total result set size
   - No automatic cleanup until `close()`

2. **Manual fetchone/fetchmany calls**:
   - Consumed rows remain in `_results` after `_index` advances past them
   - User must manually call `clear_changelog_buffer()` (undiscovered API)
   - Memory retention scales with pages fetched

3. **Append-only streaming queries**:
   - No compressor involvement means no automatic cleanup
   - Long-running streaming queries accumulate all events forever
   - Can cause OOM for indefinitely running streams

### What PEP-249 Allows

Per [PEP-249 analysis](pep-0249.rst), a cursor **CAN** free memory for already-delivered rows:

- ✅ No requirement to retain delivered rows for re-reading
- ✅ Scrolling (backward navigation) is optional
- ✅ Only `close()` makes cursor unusable - before that, fetching can continue
- ✅ Must maintain cursor state (description, rowcount, position for unfetched rows)

**Key insight**: Once rows in `_results[0:_index]` have been consumed, they can be safely discarded without violating DB-API semantics.

---

## Proposed Solution: Switch to deque for Automatic Memory Freeing

### Core Mechanism

Replace `list`-based `_results` buffer with `collections.deque` and eliminate the `_index` tracking pattern. This enables **automatic, incremental memory freeing** as rows are consumed without any explicit trimming logic.

**Key insight**: `deque.popleft()` is O(1) and immediately frees memory when blocks are exhausted, eliminating the need for periodic trimming.

### Current Implementation (list + index)

```python
from typing import TypeVar

ProcessorOutput = TypeVar("ProcessorOutput")

class ChangelogProcessor:
    _results: list[ProcessorOutput]  # All fetched rows
    _index: int                       # Position of next unconsumed row

    def __init__(self, ...):
        self._results = []
        self._index = 0

    def _consume_from_buffer(self, limit: int) -> list[ProcessorOutput]:
        # Consume rows but leave them in _results
        actual_limit = min(limit, self._remaining)
        results = self._results[self._index : self._index + actual_limit]
        self._index += actual_limit  # Move index forward
        return results  # Old rows remain in memory at _results[0:_index]

    @property
    def _remaining(self) -> int:
        return len(self._results) - self._index
```

**Problem**: Consumed rows `_results[0:_index]` remain in memory indefinitely.

### New Implementation (deque without index)

```python
from collections import deque
from typing import TypeVar

ProcessorOutput = TypeVar("ProcessorOutput")

class ChangelogProcessor:
    _results: deque[ProcessorOutput]  # Unconsumed rows only

    def __init__(self, ...):
        self._results = deque()
        # No _index needed!

    def _consume_from_buffer(self, limit: int) -> list[ProcessorOutput]:
        # Remove and return rows from front of deque
        actual_limit = min(limit, len(self._results))
        consumed = []
        for _ in range(actual_limit):
            consumed.append(self._results.popleft())  # O(1), frees memory
        return consumed

    @property
    def _remaining(self) -> int:
        return len(self._results)  # All items in deque are unconsumed
```

**Solution**: `popleft()` removes consumed rows immediately. Memory freed incrementally every ~64 rows (when a block is exhausted).

## Implementation Plan

### Changes to ChangelogProcessor

**File**: `src/confluent_sql/changelog.py`

#### 1. Import deque

```python
from collections import deque
```

#### 2. Update type annotations

```python
class ChangelogProcessor(Generic[ProcessorOutput], abc.ABC):
    _results: deque[ProcessorOutput]
    """The deque of unconsumed results fetched from the server."""
```

Remove `_index` from class documentation and type hints.

#### 3. Update `__init__`

```python
def __init__(
    self,
    connection: Connection,
    statement: Statement,
    execution_mode: ExecutionMode,
    as_dict: bool = False,
):
    self._connection = connection
    self._statement = statement
    self._execution_mode = execution_mode
    self._as_dict = as_dict

    self._next_page = None
    self._fetch_next_page_called = False
    self._most_recent_results_fetch_time = None

    self._results = deque()  # Changed from []
    # Remove: self._index = 0
    self._metrics = FetchMetrics()
```

#### 4. Update `_consume_from_buffer`

```python
def _consume_from_buffer(self, limit: int) -> list[ProcessorOutput]:
    """
    Consume up to 'limit' results from the deque buffer.

    Rows are removed from the buffer via popleft(), freeing memory incrementally.

    Args:
        limit: Maximum number of results to consume from buffer.

    Returns:
        List of up to 'limit' results from the buffer.
    """
    actual_limit = min(limit, len(self._results))
    consumed = []
    for _ in range(actual_limit):
        consumed.append(self._results.popleft())  # O(1), frees memory
    return consumed
```

#### 5. Update `_remaining` property

```python
@property
def _remaining(self) -> int:
    """Number of results remaining in current buffer."""
    return len(self._results)
```

Remove the error check since we no longer have `_index` to go out of bounds.

#### 6. Update `__next__` implementation

```python
def __next__(self) -> ProcessorOutput:
    """Implementation of iterator protocol.

    This method implements blocking iteration behavior:
    - When the buffer is empty, it calls _fetch_next_page() to get more data
    - Raises StopIteration only when no more results will ever be available
    - In streaming mode, this means iteration will block/wait for new data

    Note: This blocking behavior differs from fetchone/fetchmany in streaming
    mode, which return immediately with None/empty list if no data is buffered.
    """
    if len(self._results) == 0:
        if self._fetch_next_page_called and not self._next_page:
            raise StopIteration
        self._fetch_next_page()
        if len(self._results) == 0:
            raise StopIteration

    # Remove and return the next row from front of deque
    return self._results.popleft()  # O(1), frees memory
```

#### 7. Remove `clear_buffer` method

**Remove entirely** - no longer needed with automatic deque memory freeing:

```python
# DELETE THIS METHOD:
# def clear_buffer(self) -> None:
#     ...
```

**Rationale for deletion**:

- With deque, consumed rows are freed automatically via `popleft()`
- The only call site is `ChangelogCompressor.snapshots()` where the buffer is already empty when called (all rows consumed via `fetchmany()`)
- No legacy API users to maintain compatibility with
- Simpler, cleaner API without manual memory management methods
- Removing dead code prevents confusion about when/why to call it

#### 8. Remove `clear_changelog_buffer` from Cursor

**File**: `src/confluent_sql/cursor.py`

Remove the `clear_changelog_buffer()` method entirely:

```python
# DELETE THIS METHOD:
# def clear_changelog_buffer(self) -> None:
#     """Clear the internal changelog event buffer and reset position."""
#     if self._changelog_processor:
#         self._changelog_processor.clear_buffer()
```

#### 9. Remove call from ChangelogCompressor

**File**: `src/confluent_sql/changelog_compressor.py`

Remove the redundant call to `clear_changelog_buffer()`:

```python
def snapshots(self, fetch_batchsize: int | None = None) -> Generator[list[T], None, None]:
    while True:
        # ... check may_have_results ...

        # Fetch all currently available events
        while True:
            batch = self._cursor.fetchmany(batchsize)
            if not batch:
                # No more events available in this iteration
                # REMOVE THIS LINE: self._cursor.clear_changelog_buffer()
                break

            for changelogged_row in batch:
                op, row = cast(ChangeloggedRow, changelogged_row)
                self._apply_operation(op, cast(T, row))

        # Yield current snapshot
        yield self._copy_accumulated_rows()
```

#### 10. Update `_fetch_next_page` (no trimming needed)

The `_fetch_next_page()` method remains largely the same, just appending to deque:

```python
def _fetch_next_page(self) -> None:
    """Fetch and process the next page of results."""

    # No trimming needed - deque automatically frees memory!

    if not self._statement.is_ready:
        raise InterfaceError("Statement is not ready for result fetching.")

    if not self._statement.schema:
        raise InterfaceError("Trying to fetch results for a non-query statement")

    if not self._results or self._next_page is not None:
        # ... pacing logic unchanged ...

        self._metrics.prep_for_fetch()

        # Get raw ChangelogRow results from connection
        results, next_page = self._connection._get_statement_results(
            self._statement.name, self._next_page
        )

        self._metrics.record_fetch_completion(len(results))
        self._next_page = next_page

        # Process each changelog row just fetched
        type_converter = self._statement.type_converter
        for res in results:
            decoded_row = type_converter.to_python_row(res.row)

            if self._as_dict:
                decoded_row = self._map_row_to_dict(decoded_row)

            # Retain in deque (appends to end)
            self._retain(res.op, decoded_row)

    self._fetch_next_page_called = True
    self._most_recent_results_fetch_time = time.monotonic()
```

#### 11. Update `_retain` implementations

No changes needed - both `AppendOnlyChangelogProcessor` and `RawChangelogProcessor` call `self._results.append()`, which works identically for both `list` and `deque`.

#### 12. Update Cursor.close() method

**File**: `src/confluent_sql/cursor.py`

Update the `close()` method to remove reference to `clear_buffer()`:

```python
def close(self) -> None:
    """Close the cursor and free associated resources."""
    if not self._closed:
        if self._statement is not None and self._statement.is_deletable:
            try:
                self.delete_statement()
            except Exception as e:
                logger.error(f"Error deleting statement {self._statement.name}: {e}")

        self.rowcount = -1
        self._closed = True
        # Memory automatically freed via deque - no need to clear manually
        # Remove: self._results = []
        # Remove: self._index = 0
        self._changelog_processor = None
```

**Note**: The cursor `close()` currently sets `self._changelog_processor = None`, which automatically releases the deque. No changes needed there.

---

## Edge Cases and Considerations

### 1. Empty Buffer

**Scenario**: `len(self._results) == 0` (no unconsumed rows)
**Solution**: `popleft()` raises `IndexError`, but we always check `len()` before calling

```python
if len(self._results) == 0:
    # Fetch more or raise StopIteration
```

No special handling needed.

### 2. Single Row Fetches

**Scenario**: `fetchone()` called repeatedly
**Solution**: Each call does `popleft()` once - O(1) operation

- Memory freed incrementally every ~64 rows
- No batching or trimming logic needed

### 3. Large Batch Fetches

**Scenario**: `fetchmany(10000)` with only 100 rows buffered
**Solution**: Loop terminates after 100 `popleft()` calls

```python
for _ in range(min(10000, len(self._results))):
    consumed.append(self._results.popleft())
```

Returns whatever is available, same semantics as before.

### 4. Iteration Mid-Page

**Scenario**: Iterator paused mid-page (e.g., `next(cursor)` called once)
**Solution**: Single `popleft()` removes one row

- Memory freed when that row's block is exhausted
- No explicit trimming needed

### 5. fetchall() Behavior

**Scenario**: User calls `fetchall()` which iterates through all results
**Solution**: Rows are `popleft()`-ed as they're yielded by iteration

- Memory freed incrementally every ~64 rows
- Peak memory: ~1-2 blocks (~128 rows) instead of full result set
- Huge savings for large result sets

### 6. Backward Compatibility

**Scenario**: Existing code relies on buffer retention
**Solution**: This should not break anything because:

- PEP-249 doesn't guarantee row retention
- No scrolling support (forward-only iteration)
- Delivered rows can't be re-fetched anyway
- `deque` and `list` have same append/len/clear semantics
- Only difference: `popleft()` instead of index-based access (internal detail)

### 7. Performance Characteristics

**Scenario**: Concern about deque vs list performance
**Solution**:

- `append()`: Same O(1) for both
- `popleft()`: O(1) for deque, O(N) for list
- `len()`: O(1) for both
- Iteration: Slightly slower for deque but still O(1) per element
- **Net result**: Better performance overall due to no copying

### 8. Index-Based Access (Not Used)

**Scenario**: Code that uses `_results[i]` for random access
**Solution**:

- Audit shows we only use:
  - `_results[_index]` - replaced by `popleft()`
  - `_results[_index:]` - replaced by loop with `popleft()`
  - `_results.append()` - works identically
- deque supports indexing (`_results[0]`) but we don't need it
- All access patterns are sequential, perfect for deque

### 9. Thread Safety (Not Affected)

**Scenario**: Concern about thread safety with deque
**Solution**:

- deque operations are atomic in CPython (like list)
- We don't claim thread safety anyway (cursors are not thread-safe per DB-API)
- No change in thread safety guarantees

---

## Migration Considerations

### What Changes for Users?

**Answer**: Nothing!

- Public API remains identical
- Fetch methods work exactly the same
- Iteration behavior unchanged
- Performance improves (faster + less memory)
- No configuration or code changes needed

### What Changes for Tests?

**Minimal changes**:

- Tests that inspect `cursor._changelog_processor._results` directly will still work
  - `len(_results)` works for both list and deque
  - Iteration works the same
- Tests that inspect `_index` will need updates (but `_index` is private, shouldn't be tested)
- Most tests should pass without modification

### What Could Go Wrong?

**Unlikely issues**:

1. **Test assumptions about buffer retention**:
   - Some tests might assume rows stay in buffer after consumption
   - Fix: Update test to consume into a list if needed

2. **Performance regressions**:
   - deque is slightly slower for indexing (but we don't use indexing)
   - Fix: Profile and optimize if needed (unlikely)

**Mitigation**: Comprehensive test suite catches issues before production

---

## Memory Impact Analysis

### Before: Unbounded Growth with list + index

```
Result Set: 1,000,000 rows across 1,000 pages (1,000 rows/page)

Memory consumption as iteration progresses:
- After page 1:     1,000 rows in memory
- After page 10:   10,000 rows in memory
- After page 100: 100,000 rows in memory
- After page 1000: 1,000,000 rows in memory (all retained until close())

Peak memory: O(total_rows) = 1,000,000 rows
Memory overhead: 1,000,000 × 8 bytes/ref = 8 MB (just references)
```

### After: Incremental Freeing with deque

```
Result Set: 1,000,000 rows across 1,000 pages (1,000 rows/page)

Memory consumption as iteration progresses:
- After page 1, 0 rows consumed:    1,000 rows in deque (~16 blocks)
- After page 1, 64 rows consumed:   936 rows in deque (~15 blocks, 1 block freed)
- After page 1, 128 rows consumed:  872 rows in deque (~14 blocks, 2 blocks freed)
- After page 1, 1000 rows consumed: 0 rows in deque (all blocks freed)
- After page 2, 64 rows consumed:   936 rows in deque
... pattern repeats for each page

Peak memory during consumption: 0-1,000 rows (at most 1 unconsumed page)
Memory overhead: ~16 blocks × 16 bytes = ~256 bytes (deque structure)

Peak memory: O(page_size) = 1,000 rows maximum
Memory reduction: 99.9% for large result sets
```

### deque Block-Level Memory Freeing

```
Page size: 1,000 rows (fits in ~16 blocks of 64 elements each)

Block 0:  [rows 0-63]    ← consumed via popleft() → FREED after row 63
Block 1:  [rows 64-127]  ← consumed via popleft() → FREED after row 127
Block 2:  [rows 128-191] ← consumed via popleft() → FREED after row 191
...
Block 15: [rows 960-999] ← consumed via popleft() → FREED after row 999

Memory freed incrementally every 64 rows consumed.
No periodic trimming or bulk copying required.
```

### Comparison: fetchall() on Large Result Set

```
Scenario: fetchall() on 10,000,000 rows

Old (list + index):
- All 10M rows fetched into list
- All 10M rows remain in memory until iteration complete
- Peak: 10,000,000 rows in memory
- Memory: ~80 MB (just references) + actual row objects

New (deque):
- Rows fetched in pages (e.g., 1,000 at a time)
- Rows consumed via iteration and freed incrementally
- Only 1 page + partially consumed blocks in memory at any time
- Peak: ~2,000 rows in memory
- Memory: ~16 KB (just references) + actual row objects
- Reduction: 99.98% memory reduction for references
```

---

## Testing Strategy

### Unit Tests for deque Refactoring

#### 1. Test deque consumption behavior

```python
def test_consume_from_buffer_uses_popleft():
    """Verify _consume_from_buffer removes rows from deque."""
    processor = AppendOnlyChangelogProcessor(...)

    # Populate deque with test rows
    processor._results.extend([row1, row2, row3])

    # Consume 2 rows
    consumed = processor._consume_from_buffer(2)

    assert consumed == [row1, row2]
    assert len(processor._results) == 1  # Only row3 remains
    assert processor._results[0] == row3
```

#### 2. Test \_remaining property

```python
def test_remaining_returns_deque_length():
    """Verify _remaining returns current deque length."""
    processor = AppendOnlyChangelogProcessor(...)

    assert processor._remaining == 0

    processor._results.append(row1)
    assert processor._remaining == 1

    processor._results.popleft()
    assert processor._remaining == 0
```

#### 3. Test iteration with deque

```python
def test_iteration_uses_popleft():
    """Verify __next__ consumes rows from deque."""
    processor = AppendOnlyChangelogProcessor(...)
    processor._results.extend([row1, row2, row3])
    processor._fetch_next_page_called = True
    processor._next_page = None

    assert next(processor) == row1
    assert len(processor._results) == 2  # row1 removed

    assert next(processor) == row2
    assert len(processor._results) == 1  # row2 removed
```

#### 4. Test fetchone/fetchmany with empty deque

```python
def test_fetchone_on_empty_deque():
    """Verify fetchone returns None when deque is empty."""
    processor = AppendOnlyChangelogProcessor(...)
    processor._fetch_next_page_called = True
    processor._next_page = None

    assert processor.fetchone() is None

def test_fetchmany_on_empty_deque():
    """Verify fetchmany returns empty list when deque is empty."""
    processor = AppendOnlyChangelogProcessor(...)
    processor._fetch_next_page_called = True
    processor._next_page = None

    assert processor.fetchmany(10) == []
```

#### 5. Test \_fetch_next_page appends to deque

```python
def test_fetch_next_page_appends_to_deque():
    """Verify _fetch_next_page appends results to deque."""
    # Mock connection to return results
    processor = AppendOnlyChangelogProcessor(...)

    # Fetch a page
    processor._fetch_next_page()

    # Verify results appended to deque
    assert len(processor._results) > 0
    assert isinstance(processor._results, deque)
```

### Integration Tests

#### 1. Large result set iteration

```python
def test_large_result_set_memory_bounded():
    """Verify memory doesn't grow unbounded during iteration."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM large_table")  # 100K rows

    rows_consumed = 0
    for row in cursor:
        rows_consumed += 1
        # Check that buffer size stays bounded
        if rows_consumed % 1000 == 0:
            buffer_size = len(cursor._changelog_processor._results)
            assert buffer_size < 2000  # At most ~2 pages

    assert rows_consumed == 100000
```

#### 2. Streaming query memory behavior

```python
def test_streaming_query_incremental_freeing():
    """Verify streaming queries free memory incrementally."""
    cursor = conn.streaming_cursor()
    cursor.execute("SELECT * FROM streaming_source")

    # Consume rows and verify buffer doesn't grow
    for i in range(10000):
        row = cursor.fetchone()
        if row:
            buffer_size = len(cursor._changelog_processor._results)
            assert buffer_size < 1000  # Bounded by page size
```

#### 3. ChangelogCompressor compatibility

```python
def test_changelog_compressor_with_deque():
    """Verify ChangelogCompressor works with deque-based processor."""
    cursor = conn.streaming_cursor()
    cursor.execute("SELECT key, COUNT(*) FROM events GROUP BY key")

    compressor = cursor.changelog_compressor()

    # Get a snapshot
    snapshots = compressor.snapshots()
    snapshot = next(snapshots)

    # Verify snapshot is correct
    assert isinstance(snapshot, list)
    assert len(snapshot) > 0

    # Verify cursor buffer is empty (automatically cleared via popleft)
    assert len(cursor._changelog_processor._results) == 0
```

### Regression Tests

Run existing test suite to ensure no breaking changes:

```bash
pytest tests/unit/test_changelog_unit.py -v
pytest tests/unit/test_cursor_unit.py -v
pytest tests/unit/test_changelog_compressor_unit.py -v
pytest tests/integration/test_cursor.py -v
```

**Expected**: All existing tests pass without modification (deque is compatible with list operations used in tests).

### Memory Profiling Tests

#### 1. Before/after memory comparison

```python
import tracemalloc

def test_memory_usage_improvement():
    """Compare memory usage before and after deque implementation."""
    tracemalloc.start()

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM large_table LIMIT 100000")

    # Consume all rows
    for row in cursor:
        pass

    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    # With deque, peak should be much lower than with list
    # Peak should be proportional to page size, not total rows
    assert peak < 10_000_000  # Reasonable bound for 100K rows
```

#### 2. Long-running streaming query

```python
def test_streaming_memory_stability():
    """Verify memory remains stable during long-running streaming query."""
    import time

    cursor = conn.streaming_cursor()
    cursor.execute("SELECT * FROM streaming_source")

    memory_samples = []

    for i in range(10000):
        row = cursor.fetchone()
        if row and i % 1000 == 0:
            # Sample memory usage
            buffer_size = len(cursor._changelog_processor._results)
            memory_samples.append(buffer_size)

    # Verify memory doesn't grow over time
    assert max(memory_samples) < 1000  # Bounded
    assert max(memory_samples) - min(memory_samples) < 500  # Stable
```

---

## Implementation Approach

### Single-Phase deque Refactoring

**Goal**: Replace list + index with deque for automatic incremental memory freeing

**Changes**:

**In `src/confluent_sql/changelog.py`**:

1. ✅ Add `from collections import deque` import
2. ✅ Change `self._results = []` → `self._results = deque()`
3. ✅ Remove `self._index = 0` from `__init__`
4. ✅ Update `_consume_from_buffer()` to use `popleft()` in loop
5. ✅ Update `_remaining` property to return `len(self._results)`
6. ✅ Update `__next__()` to use `popleft()` instead of indexing
7. ✅ **Delete** `clear_buffer()` method entirely
8. ✅ Update type hints: `_results: deque[ProcessorOutput]`
9. ✅ Update docstrings to reflect deque semantics

**In `src/confluent_sql/cursor.py`**: 10. ✅ **Delete** `clear_changelog_buffer()` method entirely

**In `src/confluent_sql/changelog_compressor.py`**: 11. ✅ **Remove** call to `cursor.clear_changelog_buffer()` in `snapshots()` method

**Advantages**:

- ✅ Automatic memory freeing (no explicit trimming logic)
- ✅ Better performance (O(1) popleft vs O(N) slice copy)
- ✅ Simpler code (no trimming methods, no index tracking)
- ✅ Incremental freeing (every ~64 rows, not batch)
- ✅ No configuration needed
- ✅ No manual memory management API (removed `clear_buffer()` and `clear_changelog_buffer()`)
- ✅ Cleaner API surface (no confusing methods to learn/document)

**Code diff**:

- `changelog.py`: ~30-40 lines modified, ~10 lines deleted (clear_buffer method)
- `cursor.py`: ~10 lines deleted (clear_changelog_buffer method)
- `changelog_compressor.py`: 1 line deleted (call to clear_changelog_buffer)
- **Total**: ~40-50 lines modified/deleted across 3 files

**Estimated effort**: 1-2 hours implementation + 2-3 hours testing

---

## Rollout Plan

### Step 1: Implement deque Refactoring

- Implement all changes in `src/confluent_sql/changelog.py`
- Update type hints and docstrings
- Self-review changes for correctness

### Step 2: Add Unit Tests

- Add deque-specific unit tests (see Testing Strategy section)
- Run existing unit test suite to verify no regressions
- Fix any broken tests (should be minimal/none)
- Verify test coverage remains high

### Step 3: Run Integration Tests

- Run full integration test suite
- Add memory profiling test if needed
- Verify ChangelogCompressor still works correctly
- Test streaming queries for bounded memory

### Next Steps

1. Implement deque refactoring in `src/confluent_sql/changelog.py`
2. Add unit tests for deque-specific behavior
3. Run full test suite (unit + integration)

---

## Summary of Changes

| File                                        | Changes                                                                                         | Lines Modified/Deleted    |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------- | ------------------------- |
| `src/confluent_sql/changelog.py`            | Replace list with deque, remove `_index`, update consumption logic, **delete `clear_buffer()`** | ~40 modified, ~10 deleted |
| `src/confluent_sql/cursor.py`               | **Delete `clear_changelog_buffer()` method**                                                    | ~10 deleted               |
| `src/confluent_sql/changelog_compressor.py` | **Remove call to `clear_changelog_buffer()`**                                                   | 1 deleted                 |
| Tests (new)                                 | Add deque-specific unit tests                                                                   | ~40-80 added              |

**Total code changes**: ~40 modified + ~21 deleted + ~40-80 test lines = ~100-140 lines

**Files impacted**: 3 source files + new tests

**Breaking changes**:

- Removed `ChangelogProcessor.clear_buffer()` (internal method, not public API)
- Removed `Cursor.clear_changelog_buffer()` (public method, but no legacy users)

**User-facing changes**: None for typical usage (automatic memory management is better)

---

## Implementation Progress

- [x] **changelog.py**: Add `from collections import deque` import
- [x] **changelog.py**: Change `_results` type annotation from `list` to `deque`
- [x] **changelog.py**: Change `self._results = []` → `self._results = deque()` in `__init__`
- [x] **changelog.py**: Remove `self._index = 0` from `__init__`
- [x] **changelog.py**: Remove `_index` class-level docstring/annotation
- [x] **changelog.py**: Update `_consume_from_buffer()` to use `popleft()` loop
- [x] **changelog.py**: Update `_remaining` property to return `len(self._results)`
- [x] **changelog.py**: Update `__next__()` to use `popleft()` instead of index
- [x] **changelog.py**: Update `_fetch_next_page()` guard condition (no change needed — `not deque()` has same semantics)
- [x] **changelog.py**: Delete `clear_buffer()` method entirely
- [x] **cursor.py**: Delete `clear_changelog_buffer()` method entirely
- [x] **cursor.py**: Remove `self._index = 0` and `self._results = []` from `execute()` reset
- [x] **cursor.py**: Remove `self._index = 0` and `self._results = []` from `close()`
- [x] **changelog_compressor.py**: Remove `self._cursor.clear_changelog_buffer()` call in `snapshots()`
- [x] **changelog_compressor.py**: Update `snapshots()` docstring (remove memory management paragraph)
- [x] **test_changelog_unit.py**: Remove all `_index = 0` reset lines
- [x] **test_changelog_unit.py**: Remove `TestClearBuffer` test class
- [x] **test_changelog_compressor_unit.py**: Remove `TestCursorClearChangelogBuffer` test class
- [x] **test_changelog_compressor_unit.py**: Remove `TestGetSnapshotCallsClearBuffer` test class
- [x] **test_changelog_compressor_unit.py**: Remove unused `RawChangelogProcessor` import
- [x] Fix infinite loop in test_iteration_calls_fetch_next_page - Added `_fetch_next_page_called = True` to mock
- [x] Fix infinite loop in test_iteration_with_dict_mode - Added `_fetch_next_page_called = True` to mock
- [x] Fix infinite loop in test_iteration_stops_when_no_more_results - Added `_fetch_next_page_called = True` to mock
- [x] Fix infinite loop in test_fetch_next_page_called_during_iteration - Added `_fetch_next_page_called = True` to mock
- [x] Fix test_close_handles_statement_delete_error - Removed invalid `_results` assertion
- [x] **All unit tests pass** (160 tests: 33 changelog + 75 compressor + 52 cursor)
