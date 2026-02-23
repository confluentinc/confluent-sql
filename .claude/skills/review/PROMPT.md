# PR Review Skill

You are performing a thorough code review of a pull request. Your goal is to identify critical issues, design problems, and improvement opportunities.

## Review Process

1. **Get the diff**: Run `git diff streaming_changelog_compressor...HEAD --stat` to see what changed
2. **Read the PR description**: Look for context about what this change accomplishes
3. **Check for critical flaws first**:
   - Logic errors that would cause incorrect behavior
   - Race conditions or synchronization issues
   - Memory leaks or resource management problems
   - Security vulnerabilities
4. **Review design patterns**:
   - Consistency with existing codebase patterns
   - Appropriate use of abstractions
   - API design and backward compatibility
5. **Check testing**:
   - Are critical paths covered by tests?
   - Are edge cases tested?
   - Do tests validate the behavior claimed in the PR description?
6. **Documentation and clarity**:
   - Are subtle implementation details documented?
   - Are complex algorithms explained?
   - Would a future maintainer understand why this design was chosen?

## Important Guidelines

- **Verify assumptions**: Before flagging something as a bug, verify that your understanding of the library/language behavior is correct. Don't assume - test or look it up.
- **Avoid duplicate comments**: If you've already mentioned an issue in your review, don't repeat it.
- **Be specific**: Point to exact line numbers and code snippets when discussing issues.
- **Prioritize**: Start with critical issues, then design concerns, then nice-to-haves.
- **Run tests**: After your review, run relevant tests to ensure the changes don't break anything: `pytest tests/unit/ -q`

## Output Format

Structure your review as:

```
## Summary
[Overall assessment of the change]

## Critical Issues
- [Issue 1 with line numbers]
- [Issue 2 with line numbers]

## Design & Architecture
- [Design concern 1]
- [Design concern 2]

## Testing
- [Testing gaps or concerns]

## Documentation
- [Documentation improvements needed]

## Verdict
[Recommendation: Approve, Request Changes, or Needs Discussion]
```

## Example Response

If no issues found:
"This PR looks solid. The deque refactor is well-coordinated with the termination logic changes. All tests pass. Consider adding a comment explaining why the _fetch_next_page_called flag is needed for future maintainers."
