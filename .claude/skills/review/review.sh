#!/bin/bash
# PR Review Helper Script
# Usage: ./review.sh [branch] [parent-branch]

BRANCH="${1:=$(git rev-parse --abbrev-ref HEAD)}"
PARENT_BRANCH="${2:-streaming_changelog_compressor}"

echo "📋 PR Review for branch: $BRANCH"
echo "📍 Parent branch: $PARENT_BRANCH"
echo ""

echo "🔍 Analyzing changes..."
git diff "$PARENT_BRANCH"..."$BRANCH" --stat

echo ""
echo "📝 Running review through Claude Code..."
echo ""

# Invoke Claude with the review skill
claude -p "Review the changes from $PARENT_BRANCH to $BRANCH. Focus on:
1. Critical logic flaws that would cause incorrect behavior
2. Race conditions or synchronization issues
3. Memory management and resource handling
4. Design pattern consistency with existing code
5. Test coverage gaps for the changed functionality
6. Documentation of complex or subtle implementation details

Run 'git diff $PARENT_BRANCH...$BRANCH' to see the full diff, then provide a structured review."
