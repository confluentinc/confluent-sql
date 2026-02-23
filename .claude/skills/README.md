# Claude Code Skills Directory

This directory contains custom skills (specialized prompts) for common development workflows in confluent-sql.

## Quick Start

### `/review` - Code Review
```bash
claude -p "/review"
```
Performs thorough PR review with critical flaw detection, design analysis, and test coverage validation.

### `/test` - Test Running
```bash
claude -p "/test"
```
Runs appropriate test suite, fixes failures, and verifies coverage.

## Skill Files

Each skill is a directory containing:
- `PROMPT.md` - Claude's instructions for handling this task
- `README.md` - Your usage guide
- `*.sh` - Optional helper scripts

## How Skills Work

Skills are essentially specialized prompts that encode your established practices and preferences. Instead of explaining the review process every time, you can just use `/review` and Claude knows exactly what you want:

- ✓ Verify assumptions before flagging issues
- ✓ Prioritize critical issues first
- ✓ Point to specific line numbers
- ✓ Run tests after changes
- ✓ Avoid duplicate comments

## Adding New Skills

To create a new skill, create a directory with the pattern above and the skill will be available as `/skillname`.

## See Also

- `../.claude/SKILLS.md` - Full documentation of all skills
- Each skill's `README.md` - Specific usage instructions
