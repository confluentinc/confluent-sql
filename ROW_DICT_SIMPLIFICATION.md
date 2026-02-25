# Refactoring Plan: Reduce Row/Dict Handling Duplication

**Prerequisite**: This plan assumes completion of `CHANGELOG_MODULE_RENAME.md`

## Context

After renaming, the codebase will have:

- **result_readers.py**: Two reader classes (AppendOnlyResultReader, ChangelogEventReader)
- **changelog_compressor.py**: Four compressor classes (2×2 matrix of upsert/no-upsert × tuple/dict)

Both layers currently handle tuple/dict conversion independently:

- Result readers convert JSON→Python tuple, then optionally tuple→dict (lines 495-496 in current code)
- Compressors repeat this pattern with 4 variants to handle both tuple and dict inputs

This creates unnecessary duplication: the logic for "should this be a dict or tuple?" is scattered and the compressor variants are inflated.

## Goal

Reduce tuple/dict handling complexity by consolidating the format decision into the **result_readers layer**, reducing compressor variants from 4 to 2 (just upsert vs no-upsert).

### Semantic Rationale

- **Result readers' responsibility**: Fetch pages, deserialize JSON, convert to Python types, format as requested (tuple or dict)
- **Compressors' responsibility**: Apply changelog operations to maintain state, without caring about underlying row format
- **Cursor's responsibility**: Own the `as_dict` config decision, pass it to readers, receive results in that format

Result readers are the natural place to handle output format because:

1. They already have access to the statement schema
2. They own the initial type conversion step
3. The `as_dict` parameter is already passed to reader constructors
4. Compressors shouldn't care whether they're manipulating tuples or dicts

## Design

### Current State (After CHANGELOG_MODULE_RENAME.md)

```
Cursor
  ├── result_reader: AppendOnlyResultReader | ChangelogEventReader
  │   └── Returns: Row (tuple or dict) OR ChangeloggedRow(op, row as tuple/dict)
  │
  └── changelog_compressor (optional)
      ├── UpsertColumnsTupleCompressor
      ├── UpsertColumnsDictCompressor
      ├── NoUpsertColumnsTupleCompressor
      └── NoUpsertColumnsDictCompressor
```

### Proposed State (After This Refactoring)

```
Cursor
  ├── result_reader: AppendOnlyResultReader | ChangelogEventReader
  │   └── Returns: Row (ALWAYS in cursor's requested format: tuple XOR dict)
  │              OR ChangeloggedRow(op, row in cursor's requested format)
  │
  └── changelog_compressor (optional)
      ├── UpsertColumnsCompressor
      └── NoUpsertColumnsCompressor
```

Key change: Compressors no longer split on tuple/dict — readers guarantee output is in the correct format already.

## Implementation Strategy

### 1. Create a Shared Row Formatting Utility

**File**: `result_readers.py` (in the same module as readers)

```python
class RowFormatter:
    """Converts statement result tuples to Python rows in the requested format (tuple or dict)."""

    def __init__(self, as_dict: bool, schema: Schema):
        """
        Args:
            as_dict: If True, format rows as dicts; if False, keep as tuples
            schema: The statement schema for column names (required if as_dict=True)
        """
        self._as_dict = as_dict
        self._schema = schema
        if as_dict and not schema:
            raise InterfaceError("Schema required to format rows as dicts")
        self._column_names = [col.name for col in schema.columns] if as_dict else None

    def format(self, row: StatementResultTuple) -> ResultTupleOrDict:
        """Convert a tuple row to the configured format."""
        if not self._as_dict:
            return row
        return dict(zip(self._column_names, row, strict=True))
```

### 2. Update ResultReader Base Class

**File**: `result_readers.py`

Current approach (lines 489-499):

```python
for res in results:
    decoded_row = type_converter.to_python_row(res.row)
    if self._as_dict:
        decoded_row = self._map_row_to_dict(decoded_row)
    self._retain(res.op, decoded_row)
```

New approach:

```python
# Add to __init__:
self._row_formatter = RowFormatter(self._as_dict, statement.schema)

# In _fetch_next_page:
for res in results:
    decoded_row = type_converter.to_python_row(res.row)
    formatted_row = self._row_formatter.format(decoded_row)
    self._retain(res.op, formatted_row)
```

Remove `_map_row_to_dict()` method from ResultReader (no longer needed).

### 3. Simplify Compressor Classes

**File**: `changelog_compressor.py`

Remove the 2×2 split. Replace with:

- `UpsertColumnsCompressor[T]` — generic over row type (tuple or dict)
- `NoUpsertColumnsCompressor[T]` — generic over row type (tuple or dict)

Each compressor class is still generic but no longer duplicates format-handling code. The type parameter `T` is constrained to `ResultTupleOrDict` and automatically handles both.

#### UpsertColumnsCompressor Changes

**Before** (4 classes: UpsertColumnsTupleCompressor, UpsertColumnsDictCompressor, etc.):

```python
class UpsertColumnsTupleCompressor(UpsertColumnsCompressor[StatementResultTuple]):
    def _extract_key(self, row: StatementResultTuple) -> tuple:
        return tuple(row[i] for i in self._upsert_column_indices)

class UpsertColumnsDictCompressor(UpsertColumnsCompressor[StrAnyDict]):
    def _extract_key(self, row: StrAnyDict) -> tuple:
        return tuple(row[col_name] for col_name in self._upsert_key_column_names)
```

**After** (1 class: UpsertColumnsCompressor):

```python
class UpsertColumnsCompressor(ChangelogCompressor[T], abc.ABC):
    """Handles upsert-columned statements for both tuple and dict rows."""

    def _extract_key(self, row: T) -> tuple:
        """Extract key from row, handling both tuple and dict formats."""
        if isinstance(row, dict):
            # Dict case
            return tuple(row[col_name] for col_name in self._upsert_key_column_names)
        else:
            # Tuple case
            return tuple(row[i] for i in self._upsert_column_indices)
```

#### NoUpsertColumnsCompressor Changes

**Before** (2 classes):

```python
class NoUpsertColumnsTupleCompressor(NoUpsertColumnsCompressor[StatementResultTuple]):
    pass  # Inherits all implementation

class NoUpsertColumnsDictCompressor(NoUpsertColumnsCompressor[StrAnyDict]):
    pass  # Inherits all implementation
```

**After** (1 class, no concrete subclasses):

```python
class NoUpsertColumnsCompressor(ChangelogCompressor[T], abc.ABC):
    """Handles non-upsert statements for both tuple and dict rows.

    Row matching is performed by equality comparison, works for both tuple and dict.
    """
    # Implementation unchanged — tuple/dict handling is transparent
```

The `_find_row_position()` method uses `self._rows[i] == row`, which works identically for both tuples and dicts (Python equality). No changes needed.

### 4. Update create_changelog_compressor() Factory

**File**: `changelog_compressor.py` (lines 52-87)

**Before**:

```python
if has_upsert_columns:
    if cursor.as_dict:
        return UpsertColumnsDictCompressor(cursor, statement)
    else:
        return UpsertColumnsTupleCompressor(cursor, statement)
elif cursor.as_dict:
    return NoUpsertColumnsDictCompressor(cursor, statement)
else:
    return NoUpsertColumnsTupleCompressor(cursor, statement)
```

**After**:

```python
if has_upsert_columns:
    return UpsertColumnsCompressor(cursor, statement)
else:
    return NoUpsertColumnsCompressor(cursor, statement)
```

Much simpler — the factory only decides on the upsert axis, not tuple/dict.

### 5. Update UpsertColumnsCompressor Constructor

**File**: `changelog_compressor.py`

Add precomputed column names for dict access (currently only done in UpsertColumnsDictCompressor):

```python
class UpsertColumnsCompressor(ChangelogCompressor[T], abc.ABC):
    def __init__(self, cursor: Cursor, statement: Statement):
        super().__init__(cursor, statement)

        if not statement.traits or not statement.traits.upsert_columns:
            raise InterfaceError("UpsertColumnsCompressor requires upsert columns")

        self._upsert_column_indices = statement.traits.upsert_columns

        # Precompute column names for dict access (used if rows are dicts)
        self._upsert_key_column_names = [
            self._schema.columns[i].name for i in self._upsert_column_indices
        ]

        self._rows_by_key = {}
        self._expecting_update_after = False
```

## Files to Modify

### Source Files (2)

1. **result_readers.py** (renamed from changelog.py)
   - Add `RowFormatter` class
   - Update `ResultReader.__init__()` to create formatter
   - Update `ResultReader._fetch_next_page()` to use formatter
   - Remove `_map_row_to_dict()` method
   - Remove `_map_row_to_dict()` type annotation from abstract method

2. **changelog_compressor.py**
   - Delete 3 concrete compressor classes: UpsertColumnsTupleCompressor, UpsertColumnsDictCompressor, NoUpsertColumnsTupleCompressor
   - Keep only abstract base classes and one concrete class per axis (UpsertColumnsCompressor, NoUpsertColumnsCompressor)
   - Update `UpsertColumnsCompressor._extract_key()` to handle both tuple and dict
   - Update `UpsertColumnsCompressor.__init__()` to precompute column names
   - Simplify `create_changelog_compressor()` factory from 4-way to 2-way decision
   - Update docstrings

### Test Files (1)

3. **tests/unit/test_result_readers_unit.py** (renamed from test_changelog_unit.py)
   - Add tests for `RowFormatter` class
   - Update existing tests to verify formatting happens in readers, not compressors

## Benefits

1. **Reduced class count**: 4 compressor variants → 2
2. **Clearer responsibility**: Readers own "what format?", Compressors own "how to compress?"
3. **Eliminated duplication**: `_map_row_to_dict()` logic written once
4. **Simpler factory**: `create_changelog_compressor()` goes from 4-way conditional to 2-way
5. **Easier to extend**: Adding a new row format or compressor strategy requires changes in fewer places
6. **Type safety**: Generics still enforce correctness, just at the compressor level rather than class-level

## Type Safety Analysis

This refactoring does **not water down type hints** in ways that matter to users, though internal type specificity does change slightly. Here's the detailed breakdown:

### DB-API Boundary (User-Facing)

**Cursor.fetchone(), fetchmany(), fetchall() return types remain unchanged:**

```python
# BEFORE and AFTER: unchanged
def fetchone(self) -> ResultRow | None:
def fetchmany(self, size: int) -> list[ResultRow]:
def fetchall(self) -> list[ResultRow]:
# Where ResultRow = ResultTupleOrDict | ChangeloggedRow
```

Users continue to receive:

- `ResultTupleOrDict` (tuple or dict) for append-only queries
- `ChangeloggedRow(op, ResultTupleOrDict)` for non-append-only streaming queries

✅ **No user-facing type change**

### Changelog Compressor API (Public)

**Public method signatures remain unchanged:**

```python
# BEFORE and AFTER: unchanged
def changelog_compressor(self) -> ChangelogCompressor:
    """Returns a ChangelogCompressor instance..."""

def snapshots(self, fetch_batchsize: int | None = None) -> Generator[list[T], None, None]:
    """Yields snapshots of the accumulated result set..."""
```

Users already receive the abstract `ChangelogCompressor` type, not the concrete class. The concrete class (currently `UpsertColumnsTuple/DictCompressor`, after refactoring `UpsertColumnsCompressor`) is hidden by the factory.

**However**, the generic type `T` in the return type becomes slightly less specific:

```python
# BEFORE (specific):
compressor = cursor.changelog_compressor()  # returns UpsertColumnsDictCompressor[StrAnyDict]
for snapshot in compressor.snapshots():     # mypy infers: Generator[list[StrAnyDict], None, None]
    row = snapshot[0]                       # mypy knows: StrAnyDict

# AFTER (union):
compressor = cursor.changelog_compressor()  # returns UpsertColumnsCompressor[ResultTupleOrDict]
for snapshot in compressor.snapshots():     # mypy infers: Generator[list[ResultTupleOrDict], None, None]
    row = snapshot[0]                       # mypy knows: ResultTupleOrDict (tuple | dict)
```

The user's actual runtime behavior is **identical** because `ResultTupleOrDict` is the union of the specific types. Type checking is slightly less specific, but:

1. Users can check `cursor.as_dict` to know which type they'll get
2. They can use type guards if needed: `isinstance(row, dict)` if they need dict-specific operations
3. This matches the pattern already used elsewhere (e.g., `ResultRow` is a union type)

✅ **Minimal user-facing impact; users already work with union types elsewhere in the API**

### Internal Implementation (Type Safety)

**Current internal types (very specific):**

```python
class UpsertColumnsTupleCompressor(UpsertColumnsCompressor[StatementResultTuple]):
    def _extract_key(self, row: StatementResultTuple) -> tuple:
        # row is guaranteed to be a tuple
        return tuple(row[i] for i in self._upsert_column_indices)

class UpsertColumnsDictCompressor(UpsertColumnsCompressor[StrAnyDict]):
    def _extract_key(self, row: StrAnyDict) -> tuple:
        # row is guaranteed to be a dict
        return tuple(row[col_name] for col_name in self._upsert_key_column_names)
```

**After refactoring (union types with guards):**

```python
class UpsertColumnsCompressor(ChangelogCompressor[ResultTupleOrDict]):
    def _extract_key(self, row: ResultTupleOrDict) -> tuple:
        # row could be tuple or dict; use isinstance guard
        if isinstance(row, dict):
            return tuple(row[col_name] for col_name in self._upsert_key_column_names)
        else:
            return tuple(row[i] for i in self._upsert_column_indices)
```

The isinstance guards ensure type safety. Modern type checkers (mypy, pyright) can narrow types correctly within each branch:

```python
if isinstance(row, dict):
    # Within this block, mypy narrows row: StrAnyDict
    value = row[col_name]  # ✅ type-safe dict access
else:
    # Within this block, mypy narrows row: StatementResultTuple
    value = row[i]  # ✅ type-safe tuple access
```

**Type precision trade-off:**

- **Lost**: Class-level distinction between tuple and dict specializations
- **Gained**: Unified implementation, explicit branching that tools understand
- **Net effect**: Internal types are slightly less specific, but still fully type-safe with proper guards

### Recommendation for Type Safety

The plan should include this implementation detail:

1. **Use explicit isinstance guards** (already present in the proposed code):

   ```python
   if isinstance(row, dict):
       # Handle dict
   else:
       # Handle tuple
   ```

2. **Document the union type** in affected method docstrings:

   ```python
   def _extract_key(self, row: ResultTupleOrDict) -> tuple:
       """Extract upsert key from a row.

       Args:
           row: The row data, either a tuple (if cursor.as_dict=False) or dict (if as_dict=True).

       Returns:
           A tuple of key values in column order.
       """
   ```

3. **Optional but recommended**: For maximum clarity, use a type guard helper (Python 3.10+):

   ```python
   from typing import TypeGuard

   def _is_dict_row(row: ResultTupleOrDict) -> TypeGuard[StrAnyDict]:
       return isinstance(row, dict)

   # Then use it:
   if self._is_dict_row(row):
       # mypy still narrows to StrAnyDict here
       return tuple(row[col_name] for col_name in self._upsert_key_column_names)
   ```

   However, direct isinstance checks are simpler and mypy handles them equally well.

✅ **Internal type safety is preserved with isinstance guards; no unsafe code paths exist**

## Risks and Mitigations

**Risk 1**: Performance (isinstance checks in `_extract_key()`)

- **Mitigation**: The isinstance check is O(1) per operation, not O(n). Negligible overhead.
- **Alternative**: Profile before/after; if needed, move back to separate classes.

**Risk 2**: Less explicit class names (UpsertColumnsCompressor vs UpsertColumnsTupleCompressor)

- **Mitigation**: Docstrings clarify that the compressor handles both formats.
- **Upside**: Class names are shorter and the factory clearly shows you only choose upsert vs no-upsert.

**Risk 3**: Code path divergence (tuple vs dict handling might evolve differently)

- **Mitigation**: The isinstance check makes divergence explicit and easy to spot in diffs.
- **Upside**: Unified class means changes are made once, not in multiple places.

## Implementation Order

1. Implement `RowFormatter` utility in result_readers.py
2. Update `ResultReader` base class to use formatter
3. Remove `_map_row_to_dict()` and related methods from ResultReader
4. Delete the 3 concrete compressor subclasses
5. Update `UpsertColumnsCompressor._extract_key()` to handle both tuple and dict with explicit isinstance guards
6. Add docstrings documenting the union type in affected methods (following recommendations from Type Safety section)
7. Simplify `create_changelog_compressor()` factory
8. Update `NoUpsertColumnsCompressor` docstrings (no code changes needed)
9. Update all tests and docstrings
10. Run mypy type checker to verify type safety: `mypy src/confluent_sql`
11. Run full test suite to verify equivalence

## Verification

### 1. Type Checking

```bash
pyright src/confluent_sql
```

### 2. Unit Tests

```bash
pytest tests/unit/test_result_readers_unit.py -v
pytest tests/unit/test_changelog_compressor_unit.py -v
pytest tests/unit/test_cursor_unit.py -v
```

### 3. Integration Tests

```bash
pytest tests/integration/ -v
```

### 4. Verify Behavior Unchanged

- Test that tuple rows and dict rows are both returned correctly
- Test that compressors work with both tuple and dict inputs
- Test that upsert key extraction works for both formats

### 5. Count Classes

Before: 6 compressor classes (1 abstract base, 1 intermediate abstract, 4 concrete)
After: 3 compressor classes (1 abstract base, 2 concrete)

## Notes

- This refactoring maintains backward compatibility — all public APIs remain the same
- The change is internal only (how compressors are organized)
- Cursors behave identically before and after
- The simplification makes the code easier to maintain and extend
