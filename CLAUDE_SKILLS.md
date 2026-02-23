# Claude Code Skills Guide

This project includes custom Claude Code skills that encode your established development practices. Use them to streamline code reviews, testing, and other common workflows.

## Quick Reference

```bash
# Code review - thorough analysis of PRs
claude -p "/review"

# Run tests - identifies affected tests and fixes failures
claude -p "/test"

# Run specific tests
claude -p "/test changelog"        # changelog-related tests
claude -p "/test cursor"           # cursor-related tests
claude -p "/test --coverage"       # with coverage report
```

## Available Skills

### 1. `/review` - PR Code Review

Performs a systematic code review following your established practices.

**When to use:**
- Reviewing PRs before merging
- Analyzing branch changes
- Double-checking complex code

**How to use:**
```bash
# Review current branch
claude -p "/review"

# Review with specific focus
claude -p "/review focusing on memory management"

# Review a specific file
claude -p "/review src/confluent_sql/changelog.py"
```

**What it checks:**
- ✓ Critical logic flaws (incorrect behavior, race conditions, resource leaks)
- ✓ Design patterns (consistency with codebase, appropriate abstractions)
- ✓ Test coverage (critical paths, edge cases)
- ✓ Documentation (complex logic explained, design rationale clear)
- ✓ Security issues (XSS, SQL injection, OWASP top 10)

**Key practices:**
- Verifies assumptions about library/language behavior before flagging issues
- Avoids duplicate comments
- Prioritizes critical issues first
- Points to exact line numbers when discussing code
- Runs tests after review

**Documentation:**
- See `.claude/skills/review/README.md` for detailed usage
- See `.claude/skills/review/PROMPT.md` for review guidelines

### 2. `/test` - Test Running & Fixing

Executes tests, analyzes failures, and fixes them comprehensively.

**When to use:**
- After making code changes
- Before committing
- Investigating test failures
- Ensuring coverage

**How to use:**
```bash
# Run all unit tests
claude -p "/test"

# Run tests for a module
claude -p "/test changelog"
claude -p "/test cursor"
claude -p "/test changelog_compressor"

# Run with coverage
claude -p "/test --coverage"

# Run integration tests
claude -p "/test integration"

# Run and fix failures
claude -p "/test and fix any failures"
```

**What it does:**
- ✓ Identifies which tests are affected by your changes
- ✓ Runs appropriate test suite (unit/integration)
- ✓ Analyzes test failures with root cause analysis
- ✓ Fixes failures (updates code or tests)
- ✓ Searches for similar patterns across all tests
- ✓ Verifies comprehensive coverage after fixes

**Key practices:**
- Finds ALL affected tests, not just obvious ones
- Runs the right test suite for your changes
- Actually fixes failures instead of skipping them
- Searches for similar bug patterns in other tests
- Reports clear results (pass count, fail count, coverage %)

**Documentation:**
- See `.claude/skills/test/README.md` for detailed usage
- See `.claude/skills/test/PROMPT.md` for test execution guidelines

## Why Use Skills?

Your usage data shows you spend ~50% of time on code review and ~20% on testing. These skills eliminate the need to explain the same requirements every session by encoding your established practices:

**Before:** Every review required you to explain what to look for
**After:** `/review` automatically handles your review process

**Before:** You had to specify test suites or fix test failures manually
**After:** `/test` finds affected tests, runs them, and fixes failures

**Before:** 31% of sessions involved correcting Claude's initial misunderstandings
**After:** Skills eliminate most initial misdiagnosis by providing clear context

## How Skills Work

Skills are located in `.claude/skills/` with this structure:
```
skills/
├── review/
│   ├── PROMPT.md          # Claude's instructions
│   ├── README.md          # Usage guide
│   └── review.sh          # Helper script
└── test/
    ├── PROMPT.md          # Claude's instructions
    └── README.md          # Usage guide
```

When you use `/skillname`, Claude reads the PROMPT.md file which contains:
1. Context about the task
2. Step-by-step process to follow
3. Important guidelines
4. Expected output format

You can customize skills by editing their `PROMPT.md` files. Changes take effect immediately.

## Creating New Skills

To add a skill for another common workflow:

1. Create a directory in `.claude/skills/skillname/`
2. Add `PROMPT.md` with your custom instructions
3. Add `README.md` with usage guide
4. Use it as: `claude -p "/skillname"`

Example: Creating a `/document` skill
```bash
mkdir -p .claude/skills/document
# Create PROMPT.md with documentation guidelines
# Create README.md with usage guide
# Use as: claude -p "/document"
```

## Tips for Best Results

1. **Be specific in your requests**: `/review focusing on memory management` is better than just `/review`
2. **Use the shell helper scripts**: `./.claude/skills/review/review.sh` for quick reviews
3. **Combine skills**: Run `/review` first, then `/test` to validate changes
4. **Customize as needed**: Edit PROMPT.md files to match your evolving practices
5. **Share with team**: These skills encode project-specific best practices - share them with teammates

## See Also

- `.claude/SKILLS.md` - Full technical documentation
- `.claude/skills/` - Individual skill directories
- `.claude/settings.json` - Claude Code configuration for this project

## Questions?

Each skill has a `README.md` with detailed usage instructions and examples. Start there for specific questions about how to use `/review` or `/test`.
