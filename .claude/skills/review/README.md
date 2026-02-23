# PR Review Skill

A specialized skill for thorough code reviews that follows your established review practices.

## How to Use

### In Claude Code (Interactive)
```bash
claude
# Then in the session:
/review
```

### In Claude Code (Non-interactive)
```bash
claude -p /review
```

### With Context
If you want to review a specific branch or PR:
```bash
claude -p "Review the current branch for critical flaws, design issues, and test coverage gaps"
```

## What This Skill Does

1. **Analyzes the diff** to understand what changed
2. **Checks for critical flaws** first (logic errors, race conditions, resource leaks, security issues)
3. **Reviews design patterns** for consistency and appropriateness
4. **Validates test coverage** for critical paths and edge cases
5. **Checks documentation** for clarity and maintainability

## Output

You'll get a structured review with:
- Summary assessment
- Critical issues (with line numbers)
- Design & architecture concerns
- Testing gaps
- Documentation recommendations
- Final verdict (Approve, Request Changes, or Needs Discussion)

## Key Practices Built In

This skill implements your established review practices:
- ✓ Verifies assumptions about library behavior before flagging issues
- ✓ Avoids duplicate comments
- ✓ Prioritizes critical issues first
- ✓ Points to specific line numbers
- ✓ Runs relevant tests after review
- ✓ Explains implementation rationale for subtle code

## Customization

To customize the review focus, you can:
```bash
claude -p "Review PR focusing on memory management and test coverage"
```

Or to review a specific file:
```bash
claude -p "/review for src/confluent_sql/changelog.py"
```
