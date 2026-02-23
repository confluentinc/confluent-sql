# Test Running Skill

A specialized skill for running tests and fixing failures comprehensively.

## How to Use

### Run All Unit Tests
```bash
claude -p "/test"
```

### Run Specific Test File
```bash
claude -p "/test tests/unit/test_changelog_unit.py"
```

### Run Tests for a Module
```bash
claude -p "/test changelog"
# Automatically finds and runs changelog-related tests
```

### Run with Full Coverage Report
```bash
claude -p "/test --coverage"
```

### Run Integration Tests
```bash
claude -p "/test integration"
```

## What This Skill Does

1. **Identifies affected tests** based on which files you changed
2. **Runs the appropriate test suite** (unit, integration, or specific test files)
3. **Analyzes any failures** with root cause analysis
4. **Fixes failures** by either correcting code or updating tests
5. **Verifies comprehensive coverage** by running full suite
6. **Searches for similar patterns** to ensure all affected tests are found

## Key Practices Built In

This skill implements your established testing practices:
- ✓ Finds ALL affected tests, not just obvious ones
- ✓ Runs the right test suite for your changes
- ✓ Actually fixes failures instead of skipping them
- ✓ Searches for similar bug patterns across all tests
- ✓ Verifies coverage after fixes
- ✓ Reports clear results with pass/fail counts

## Common Scenarios

### After modifying changelog.py:
```bash
claude -p "/test changelog.py"
```
→ Automatically runs `test_changelog_unit.py` and `test_changelog_compressor_unit.py`

### After modifying cursor.py:
```bash
claude -p "/test cursor"
```
→ Automatically runs `test_cursor_unit.py`

### Fixing a test failure:
```bash
claude -p "/test and fix any failures"
```
→ Runs tests, analyzes failures, and makes necessary code/test changes

### Before committing:
```bash
claude -p "/test all"
```
→ Runs full unit test suite with coverage report
