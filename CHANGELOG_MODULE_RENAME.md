# Refactoring Plan: Rename changelog.py to result_readers.py

## Context

The current naming of `changelog.py` and its classes (`AppendOnlyChangelogProcessor`, `RawChangelogProcessor`) is misleading. These classes don't actually "process" changelog streams in the sense of interpreting or applying changelog operations. Instead, they:

1. Fetch paginated results from the server
2. Convert rows from JSON to Python types
3. Buffer results in a deque
4. Expose results via iteration and fetch methods

The actual changelog processing/interpretation happens in `changelog_compressor.py`, which applies INSERT/UPDATE/DELETE operations to maintain a logical result set over time.

The rename clarifies the separation of concerns:

- **result_readers.py**: Read and buffer results from the server (no interpretation)
- **changelog_compressor.py**: Interpret and compress buffered changelog events into snapshots
- **cursor.py**: Coordinate and expose via DB-API

## Scope

### Files to Modify

**Source files (4):**

1. `/Users/jlrobins/git/confluent-sql/src/confluent_sql/changelog.py` → rename to `result_readers.py`
2. `/Users/jlrobins/git/confluent-sql/src/confluent_sql/cursor.py`
3. `/Users/jlrobins/git/confluent-sql/src/confluent_sql/changelog_compressor.py`
4. `/Users/jlrobins/git/confluent-sql/src/confluent_sql/__init__.py`

**Test files (3):** 5. `/Users/jlrobins/git/confluent-sql/tests/unit/test_changelog_unit.py` → rename to `test_result_readers_unit.py` 6. `/Users/jlrobins/git/confluent-sql/tests/unit/test_cursor_unit.py` 7. `/Users/jlrobins/git/confluent-sql/tests/unit/test_changelog_compressor_unit.py`

## Detailed Changes

### 1. Rename Module File

**Before:** `src/confluent_sql/changelog.py`
**After:** `src/confluent_sql/result_readers.py`

### 2. Rename Classes

| Old Name                       | New Name                 | Rationale                                             |
| ------------------------------ | ------------------------ | ----------------------------------------------------- |
| `ChangelogProcessor`           | `ResultReader`           | Abstract base class for reading results               |
| `AppendOnlyChangelogProcessor` | `AppendOnlyResultReader` | Reads append-only results (discards op, returns rows) |
| `RawChangelogProcessor`        | `ChangelogEventReader`   | Reads changelog events (preserves op + row)           |

**Note:** `ChangeloggedRow` keeps its name - it's accurate as a data container.

### 3. Update Cursor Data Member

In `cursor.py`, rename the instance variable:

**Before:** `self._changelog_processor: AppendOnlyChangelogProcessor | RawChangelogProcessor | None`
**After:** `self._result_reader: AppendOnlyResultReader | ChangelogEventReader | None`

Also rename the helper method:

**Before:** `def _get_changelog_processor(self) -> ChangelogProcessor[Any]:`
**After:** `def _get_result_reader(self) -> ResultReader[Any]:`

### 4. Update Import Statements

**Files importing from the module:**

- `cursor.py` (lines 17-24): Update import from `changelog` → `result_readers`
- `changelog_compressor.py` (line 38): Update import from `changelog` → `result_readers`
- `__init__.py` (line 8): Update import from `changelog` → `result_readers`
- `test_changelog_unit.py` (line 7): Update import from `changelog` → `result_readers`
- `test_cursor_unit.py` (line 6): Update import from `changelog` → `result_readers`
- `test_changelog_compressor_unit.py` (line 7): Update import from `changelog` → `result_readers`

### 5. Update Docstrings and Comments

**In result_readers.py (formerly changelog.py):**

- Module docstring: Change "Changelog fetching and conversion" → "Result reading and buffering"
- Update class docstrings to reflect "reading" vs "processing"
- Update comments referring to "changelog processor" → "result reader"
- Update references from `ChangelogProcessor` type annotation to `ResultReader`

**In cursor.py:**

- Update docstrings that mention "changelog processor" → "result reader"
- Comments at lines 121-123, 233-243 referencing the processor
- Type annotations in docstrings

**In changelog_compressor.py:**

- Update references to "changelog processor" in docstrings
- Note that it "consumes from result readers" rather than "processors"

### 6. Update Test File Names and Content

**Rename test file:**

- `tests/unit/test_changelog_unit.py` → `tests/unit/test_result_readers_unit.py`

**Update test content:**

- Fixture names: `append_only_processor` → `append_only_reader`, `raw_changelog_processor` → `changelog_event_reader`
- Test descriptions and docstrings
- Comments referring to processors
- Class name references in test assertions and mocks

### 7. Type Annotations and Variables

Update all local variables and parameters named with "processor":

- In `cursor.py`: method `_get_changelog_processor()` → `_get_result_reader()`
- In test files: fixtures, local variables, and type annotations
- Type annotations: `ChangelogProcessor[T]` → `ResultReader[T]`

## Implementation Steps

1. **Rename the module file**
   - `git mv src/confluent_sql/changelog.py src/confluent_sql/result_readers.py`

2. **Update class names in result_readers.py**
   - `ChangelogProcessor` → `ResultReader` (line 117)
   - `AppendOnlyChangelogProcessor` → `AppendOnlyResultReader` (line 505)
   - `RawChangelogProcessor` → `ChangelogEventReader` (line 547)

3. **Update module docstrings in result_readers.py**
   - Revise module-level docstring (lines 1-22) to reflect "reading" and "buffering" terminology
   - Update class docstrings to emphasize fetching/buffering vs processing
   - Update all references from "changelog processor" to "result reader"

4. **Update cursor.py**
   - Change import statement: `from .changelog` → `from .result_readers` (line 17)
   - Update class references in imports (lines 18-24)
   - Rename `self._changelog_processor` → `self._result_reader` (lines 121, 201, 237, 243, 394, 470, 474, 631, 641)
   - Rename `_get_changelog_processor()` → `_get_result_reader()` (line 233)
   - Update all calls to renamed method (lines 282, 326, 355, 367, 464)
   - Update docstrings and comments
   - Update type annotations in method signatures

5. **Update changelog_compressor.py**
   - Change import statement: `from .changelog` → `from .result_readers` (line 38)
   - Update class references in imports (line 38)
   - Update docstrings mentioning "changelog processor"

6. **Update **init**.py**
   - Change import statement: `from .changelog` → `from .result_readers` (line 8)

7. **Rename and update test files**
   - `git mv tests/unit/test_changelog_unit.py tests/unit/test_result_readers_unit.py`
   - Update imports in all test files
   - Update fixture names and return type annotations
   - Update test docstrings and comments
   - Update variable names and class references in test code
   - Update parametrize fixtures if they reference old class names

8. **Global search and replace**
   - Search for "changelog processor" in comments/docstrings → "result reader"
   - Search for references to old class names in test assertions
   - Ensure consistency across all documentation

## Verification

### 1. Run Type Checking

```bash
pyright
```

### 2. Run Unit Tests

```bash
pytest tests/unit/test_result_readers_unit.py -v
pytest tests/unit/test_cursor_unit.py -v
pytest tests/unit/test_changelog_compressor_unit.py -v
```

### 3. Run Full Test Suite

```bash
pytest tests/unit/ -v
```

### 4. Verify Public API Unchanged

The public API should remain stable - only `ChangeloggedRow` is exported from the package, and it keeps its name.

### 5. Check Import References

Ensure all imports resolve correctly:

```bash
python -c "from confluent_sql.result_readers import AppendOnlyResultReader, ChangelogEventReader, ChangeloggedRow"
python -c "from confluent_sql import ChangeloggedRow"
```

## Notes

- This is a pure refactoring - no behavioral changes
- Public API remains stable (`ChangeloggedRow` is the only exported symbol from this module)
- The rename better reflects the actual responsibility of these classes
- Improves code clarity by distinguishing "reading" from "processing/interpreting"
- The rename from `ChangelogProcessor` to `ResultReader` eliminates confusion about the layer's purpose
