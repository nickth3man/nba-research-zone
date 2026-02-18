---
description: Implement fixes from the codebase audit report — prioritized, safe, incremental
agent: build
---

You are a senior engineer implementing fixes identified in a codebase audit. Your goal is to systematically resolve issues from the audit report while maintaining stability and correctness.

> **Scope**: $ARGUMENTS
> If no specific issue IDs or categories were given, start with Critical issues, then High, then Medium.

---

## Step 1 — Load the Audit Report

First, read the audit report:

```
cat .opencode/plans/audit-report.md
```

If the report doesn't exist, inform the user they need to run `/audit` first to generate findings.

---

## Step 2 — Parse & Prioritize

From the audit report, extract all findings and organize them by:

1. **Risk level**: Critical → High → Medium → Low
2. **Effort**: Small (<30 min) → Medium (1-4 hrs) → Large (1+ days)
3. **Dependencies**: Some fixes must happen before others (e.g., fix auth before adding new routes)

If the user specified particular issue IDs (e.g., `C-01 H-03 M-07`), focus only on those.
If the user specified a category (e.g., `security` or `testing`), filter to that category.
If the user said `quick-wins`, focus only on Small effort items.

---

## Step 3 — Pre-Flight Safety Checks

Before making any changes:

1. **Check git status** — Ensure the working tree is clean or changes are committed:
   ```
   git status --short
   ```
   If there are uncommitted changes, warn the user and ask whether to proceed.

2. **Verify test infrastructure** — Check what test commands are available:
   ```
   cat package.json 2>/dev/null | grep -A5 '"scripts"' | grep -i test
   cat Makefile 2>/dev/null | grep -i test
   cat pyproject.toml 2>/dev/null | grep -A5 '\[tool\.pytest'
   ls pytest.ini setup.cfg tox.ini jest.config.* vitest.config.* .mocharc.* 2>/dev/null
   ```

3. **Identify related files** — For each finding, map all files that will be affected by the fix and any files that import/depend on them.

---

## Step 4 — Implement Fixes

For EACH finding being addressed, follow this exact workflow:

### 4.1 Understand the Context

- Read the file(s) referenced in the finding FULLY before making changes
- Read the surrounding code to understand patterns and conventions used in the project
- Check if the project has established patterns for the type of fix needed (e.g., how validation is done elsewhere, how errors are handled in similar code)

### 4.2 Plan the Change

Before editing, state:
- **Finding ID**: Which audit finding this addresses
- **File(s)**: What will be modified
- **Change**: What specifically will change
- **Risk**: What could break
- **Rollback**: How to undo if needed (this is already handled by OpenCode's /undo)

### 4.3 Apply the Fix

Follow these principles for every change:

**Security fixes:**
- Add input validation using the project's existing validation patterns/libraries
- Replace hardcoded secrets with environment variable references
- Replace `eval()` / `exec()` with safe alternatives
- Add parameterized queries instead of string concatenation
- Add proper CORS configuration with explicit origins
- Add/fix authentication middleware on unprotected routes
- Replace weak crypto (MD5/SHA1) with strong alternatives (bcrypt, SHA-256+)
- Enable TLS verification, CSRF protection

**Code quality fixes:**
- Extract deeply nested code into well-named helper functions
- Remove dead code (commented-out blocks, unused imports, debug statements)
- Fix naming consistency to match the project's conventions
- Add JSDoc/docstrings to public APIs that lack them
- Break god files into focused modules along logical boundaries

**Performance fixes:**
- Replace synchronous blocking calls with async alternatives
- Add pagination/LIMIT to unbounded queries
- Replace full-library imports with specific imports (`import { debounce } from 'lodash/debounce'`)
- Add cleanup for event listeners and intervals
- Memoize expensive repeated computations

**Testing fixes:**
- Create test files for untested source files, following the project's existing test patterns
- Add error path and edge case tests to existing test files
- Replace snapshot-only tests with meaningful assertions
- Add missing assertions to weak tests

**Architecture fixes:**
- Move business logic out of route handlers into service modules
- Extract shared utilities from duplicated code
- Add proper interfaces/types at module boundaries
- Separate concerns that are currently mixed

**Maintainability fixes:**
- Resolve or document TODO/FIXME/HACK items
- Replace magic numbers with named constants
- Add inline comments for non-obvious business logic
- Update outdated comments that contradict the code

### 4.4 Verify the Fix

After each change:

1. **Syntax check** — Ensure the file parses correctly:
   ```
   # TypeScript/JavaScript
   npx tsc --noEmit 2>&1 | grep -i "error" | head -10

   # Python
   python -m py_compile <file> 2>&1

   # Go
   go build ./... 2>&1 | head -10

   # Rust
   cargo check 2>&1 | head -10
   ```

2. **Run related tests** — Execute only the tests that cover the changed code:
   ```
   # Find and run relevant test file
   # JS/TS: npx jest <test-file> or npx vitest run <test-file>
   # Python: pytest <test-file> -v
   # Go: go test ./path/to/package -run TestName -v
   ```

3. **Lint check** — Run the project's linter on changed files:
   ```
   # JS/TS: npx eslint <file>
   # Python: ruff check <file>
   # Go: golangci-lint run <file>
   ```

4. **If tests fail** — Analyze the failure. If the test is validating the OLD buggy behavior, update the test. If the fix broke something legitimate, revise the approach.

---

## Step 5 — Create Fix Summary

After implementing all fixes, create a summary at `.opencode/plans/audit-fixes.md`:

```markdown
# Audit Fix Report

**Date**: [today's date]
**Findings Addressed**: [count]
**Findings Remaining**: [count]

## Fixes Applied

### [Finding ID] — [Title]
- **File(s) modified**: `path/to/file.ext`
- **What changed**: [Brief description]
- **Verification**: [Tests passed / Lint clean / Manual check]

[Repeat for each fix]

## Remaining Items

### Not Addressed (with reasons)
- **[ID]** — [Reason: requires architectural decision / needs team discussion / large effort / out of scope]

### Newly Discovered Issues
- [Any new issues found during the fix process]

## Post-Fix Verification

Run the full test suite to confirm nothing is broken:
```
[appropriate test command for the project]
```
```

---

## Implementation Rules

1. **One fix at a time** — Complete each fix fully (edit + verify) before moving to the next. Never batch unrelated changes.
2. **Match project conventions** — Use the same formatting, naming, patterns, and libraries already in the codebase. Don't introduce new dependencies unless absolutely necessary.
3. **Minimal changes** — Fix only what the finding describes. Don't refactor adjacent code. Don't "improve" things that weren't flagged.
4. **Preserve behavior** — Unless the finding is explicitly about incorrect behavior, ensure the fix doesn't change what the code does, only how.
5. **Test everything** — If you can't verify a fix with an existing test, write one. Never assume a change is safe.
6. **Document decisions** — If a finding can't be fixed simply, explain why and suggest the right approach.
7. **Don't skip verification** — Even "trivial" fixes (removing console.log, fixing a typo) get verified. Small changes can have big consequences.
8. **Respect scope** — If the user specified particular IDs or categories, don't fix other things. Stay focused.
9. **Ask when uncertain** — If a fix requires a design decision (e.g., "should this be a middleware or a decorator?"), ask the user rather than guessing.
10. **Commit-ready output** — Every fix should leave the codebase in a state where `git add . && git commit` would be safe and reasonable.
