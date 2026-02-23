# Claude Code Skills for confluent-sql

This project includes custom skills (specialized prompts) to streamline common development workflows.

## Available Skills

### `/review` - PR Code Review

A thorough code review skill that identifies critical flaws, design issues, and testing gaps.

**Usage:**
```bash
claude -p "/review"
```

**What it does:**
- Analyzes pull request diffs
- Identifies critical logic flaws, race conditions, and resource leaks
- Reviews design pattern consistency
- Validates test coverage
- Checks documentation quality
- Provides structured review with verdict

**Files:**
- `skills/review/PROMPT.md` - Review guidelines and process
- `skills/review/README.md` - Usage guide
- `skills/review/review.sh` - Shell helper script

**Based on your practices:**
- Verifies assumptions before flagging issues
- Avoids duplicate comments
- Prioritizes critical issues
- Points to specific line numbers
- Runs tests after review

### `/test` - Test Running & Fixing

A comprehensive test execution skill that runs the right tests and fixes failures.

**Usage:**
```bash
claude -p "/test"              # Run all unit tests
claude -p "/test changelog"    # Run changelog-related tests
claude -p "/test --coverage"   # Run with coverage report
```

**What it does:**
- Identifies affected tests based on changed files
- Runs appropriate test suite (unit/integration)
- Analyzes test failures with root cause analysis
- Fixes failures (code or test updates)
- Verifies comprehensive coverage
- Searches for similar bug patterns

**Files:**
- `skills/test/PROMPT.md` - Test execution guidelines
- `skills/test/README.md` - Usage guide

**Based on your practices:**
- Finds ALL affected tests, not just obvious ones
- Runs the right test suite
- Fixes failures instead of skipping them
- Searches for similar patterns
- Reports clear results with pass/fail counts

## How to Use These Skills

### In Interactive Claude Sessions
```bash
cd /path/to/confluent-sql
claude
# Then type:
/review
/test
```

### In Non-Interactive Mode
```bash
claude -p "/review"
claude -p "/test"
```

### With Custom Parameters
```bash
claude -p "/review focusing on memory management"
claude -p "/test changelog.py with coverage"
```

## Why These Skills?

From your usage insights, you spend:
- **50% of time on code review** - `/review` handles this systematically
- **20% of time on testing** - `/test` handles test execution and fixes
- **Multiple interruptions per session** to redirect effort - These skills eliminate the need to reiterate common requirements

Previously, you had to:
- Explain review focus each time
- Specify which tests to run
- Repeat the same debugging steps
- Correct Claude's initial misunderstandings

Now these skills encode your established practices and expectations.

## Customizing Skills

To modify skill behavior, edit the `PROMPT.md` files in each skill directory. For example:
- `skills/review/PROMPT.md` - Modify review criteria
- `skills/test/PROMPT.md` - Modify test execution strategy

Changes take effect immediately in your next session.

## Creating New Skills

To add a new skill, create a directory with:
```
skills/skillname/
├── PROMPT.md          # Instructions for Claude
├── README.md          # Usage guide for you
└── skillname.sh       # Optional helper script
```

Then use it as: `claude -p "/skillname"`
