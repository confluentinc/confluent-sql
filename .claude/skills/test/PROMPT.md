# Test Running Skill

You are running the test suite and fixing any failures. Your goal is to ensure all tests pass.

## Test Execution Strategy

1. **Identify which tests to run**:
   - If changes are in `src/confluent_sql/changelog.py`: Run `pytest tests/unit/test_changelog_unit.py -xvs`
   - If changes are in `src/confluent_sql/cursor.py`: Run `pytest tests/unit/test_cursor_unit.py -xvs`
   - If changes are in `src/confluent_sql/changelog_compressor.py`: Run `pytest tests/unit/test_changelog_compressor_unit.py -xvs`
   - For general changes: Run all unit tests: `pytest tests/unit/ -q`
   - If integration tests are needed: `pytest tests/integration/ -xvs`

2. **Run the tests**: Execute the appropriate test command

3. **Analyze failures**:
   - Read the error message carefully
   - Identify the root cause
   - Check if it's a test issue or code issue

4. **Fix failures**:
   - If the code is wrong: Fix the code and re-run
   - If the test is wrong: Fix the test and re-run
   - If mocking is incomplete: Add necessary mocks and re-run

5. **Verify comprehensive coverage**:
   - Run full test suite: `pytest tests/unit/ -q`
   - Check coverage: `pytest tests/unit/ --cov=src/confluent_sql --cov-report=term-missing -q`

## Important Guidelines

- **Find all affected tests**: Use `grep -r "pattern" tests/` to find all tests that might be affected by your changes
- **Don't skip tests**: Never use `-k "not test_x"` to skip failing tests - fix them instead
- **Run in the right context**: Make sure you're running tests that actually exercise the code you changed
- **Check for similar patterns**: If you fix one infinite loop bug, search for similar patterns in other tests

## Output Format

Report:
```
## Test Results
- Total tests run: X
- Passed: Y
- Failed: Z
- Coverage: A%

## Issues Found & Fixed
[List any test failures you encountered and how they were fixed]

## Verification
[Confirmation that all tests pass]
```
