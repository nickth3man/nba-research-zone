---
description: Deep 6-dimension codebase audit — architecture, quality, security, performance, testing, maintainability
agent: plan
subtask: true
---

You are a comprehensive codebase auditor. Perform an exhaustive, read-only analysis of this project across six critical dimensions. You MUST NOT modify any files. Your job is to discover, analyze, and report.

> **Scope**: $ARGUMENTS
> If no scope was provided, audit the entire project.

---

## PHASE 1 — Project Discovery

Begin by mapping the project. Run each of these steps in order.

### 1.1 Structure & File Tree

Use `bash` to get the full layout:

```
find . -not -path '*/node_modules/*' -not -path '*/.git/*' -not -path '*/venv/*' -not -path '*/__pycache__/*' -not -path '*/dist/*' -not -path '*/build/*' -not -path '*/.next/*' -not -path '*/.opencode/*' | head -250
```

Then count total lines of code:

```
wc -l $(rg --files -g '!*.lock' -g '!*node_modules*' -g '!*dist*' -g '!*.map' 2>/dev/null) 2>/dev/null | tail -1
```

### 1.2 Language & Framework Detection

```
ls -la package.json requirements.txt Cargo.toml go.mod Gemfile composer.json pyproject.toml pom.xml build.gradle Makefile Dockerfile docker-compose.yml tsconfig.json 2>/dev/null
```

Check dependencies:

```
cat package.json 2>/dev/null | head -60
cat requirements.txt 2>/dev/null | head -40
cat pyproject.toml 2>/dev/null | head -60
cat go.mod 2>/dev/null | head -30
cat Cargo.toml 2>/dev/null | head -40
```

Language distribution:

```
echo "=== File counts by type ===" && \
echo "Python:     $(rg --files -t py 2>/dev/null | wc -l)" && \
echo "TypeScript: $(rg --files -t ts 2>/dev/null | wc -l)" && \
echo "JavaScript: $(rg --files -t js 2>/dev/null | wc -l)" && \
echo "Go:         $(rg --files -t go 2>/dev/null | wc -l)" && \
echo "Rust:       $(rg --files -t rust 2>/dev/null | wc -l)" && \
echo "Ruby:       $(rg --files -t ruby 2>/dev/null | wc -l)" && \
echo "Java:       $(rg --files -t java 2>/dev/null | wc -l)" && \
echo "C/C++:      $(rg --files -t c -t cpp 2>/dev/null | wc -l)" && \
echo "HTML:       $(rg --files -t html 2>/dev/null | wc -l)" && \
echo "CSS/SCSS:   $(rg --files -t css 2>/dev/null | wc -l)" && \
echo "JSON:       $(rg --files -t json -g '!*node_modules*' -g '!*.lock' 2>/dev/null | wc -l)" && \
echo "YAML:       $(rg --files -t yaml 2>/dev/null | wc -l)" && \
echo "Markdown:   $(rg --files -t md 2>/dev/null | wc -l)" && \
echo "Shell:      $(rg --files -t sh 2>/dev/null | wc -l)"
```

### 1.3 Test vs Source Mapping

```
echo "=== Test files ===" && rg --files -g '*test*' -g '*spec*' -g '*__tests__*' 2>/dev/null | sort && \
echo "=== Source files (no tests, no config) ===" && rg --files -g '!*test*' -g '!*spec*' -g '!*__tests__*' -g '!*node_modules*' -g '!*.lock' -g '!*dist*' -g '!*build*' -t py -t js -t ts -t go -t rust -t java -t rb 2>/dev/null | sort
```

### 1.4 Entry Points

```
rg -l "if __name__.*__main__|app\.(listen|run)|createServer|module\.exports|export default|func main\(\)|fn main\(\)" --sort path 2>/dev/null
```

---

## PHASE 2 — Security Sweeps (OWASP-aligned)

Run ALL of these pattern searches. Use `bash` with `rg` (ripgrep) for every search. OpenCode's grep tool uses ripgrep internally, but for complex multi-pattern sweeps, use bash directly for maximum control.

### 2.1 Hardcoded Secrets & Credentials

```
rg -i "(api[_-]?key|secret[_-]?key|password|passwd|token|auth[_-]?token|access[_-]?key|private[_-]?key)\s*[:=]\s*['\"][^'\"]{4,}['\"]" --hidden -C 1 -g '!*.lock' -g '!*node_modules*' -g '!*dist*' 2>/dev/null
```

```
rg -i "(AKIA[0-9A-Z]{16}|aws_secret|AWS_ACCESS|sk-[a-zA-Z0-9]{20,}|ghp_[a-zA-Z0-9]{36}|gho_[a-zA-Z0-9]{36})" --hidden -C 1 2>/dev/null
```

```
rg -i "-----BEGIN (RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----" --hidden 2>/dev/null
```

### 2.2 Dangerous Execution & Injection

```
rg -i "eval\s*\(|exec\s*\(|Function\s*\(|system\s*\(|subprocess\.(call|run|Popen).*shell\s*=\s*True|os\.system\s*\(|child_process" -t py -t js -t ts -t rb -C 2 2>/dev/null
```

```
rg -i "(execute|query|raw)\s*\(.*[\"'].*(%s|\$\{|\" ?\+|\' ?\+|f['\"])" -t py -t js -t ts -C 2 2>/dev/null
rg -i "SELECT.*FROM.*WHERE.*\+|INSERT.*INTO.*\+|UPDATE.*SET.*\+" -t py -t js -t ts -C 2 2>/dev/null
```

### 2.3 XSS Vectors

```
rg -i "(innerHTML|outerHTML|document\.write|dangerouslySetInnerHTML|v-html|\{\{.*\|.*safe)" -t js -t ts -t html -C 2 2>/dev/null
```

### 2.4 Insecure Crypto & TLS

```
rg -i "(md5|sha1)\s*\(|hashlib\.(md5|sha1)|createHash\s*\(\s*['\"]md5|createHash\s*\(\s*['\"]sha1" -t py -t js -t ts -C 1 2>/dev/null
```

```
rg -i "(verify\s*=\s*False|rejectUnauthorized.*false|SSL_VERIFY.*false|NODE_TLS_REJECT_UNAUTHORIZED.*0|InsecureSkipVerify.*true)" -C 2 2>/dev/null
```

### 2.5 CORS & Security Headers

```
rg -i "Access-Control-Allow-Origin.*\*|cors\(\s*\)|allowedOrigins.*\*|credentials:\s*true" -C 2 2>/dev/null
```

```
rg -i "CSRF.*disable|nosec|noinspection|#\s*noqa.*security|eslint-disable.*security" -C 2 2>/dev/null
```

### 2.6 Auth & Session

```
rg -i "(jwt|jsonwebtoken|jose)" -l 2>/dev/null
rg -i "(verify|decode|sign)\s*\(" -g '*auth*' -g '*jwt*' -g '*token*' -g '*session*' -C 3 2>/dev/null
```

---

## PHASE 3 — Complexity & Quality Hotspots

### 3.1 Deeply Nested Code (3+ indentation levels)

```
rg -n "^\s{12,}(if |for |while |switch |case |match )" --sort path -t py -t js -t ts -t go -t rust 2>/dev/null | head -30
```

### 3.2 Control Flow Density per File

```
rg -c "^\s*(if |else|elif|for |while |switch |case |catch |except )" --sort path -t py -t js -t ts -t go 2>/dev/null | sort -t: -k2 -rn | head -20
```

### 3.3 Longest Files (likely need splitting)

```
wc -l $(rg --files -t py -t js -t ts -t go -t rust -t java -t rb -g '!*node_modules*' -g '!*dist*' -g '!*.lock' -g '!*.min.*' 2>/dev/null) 2>/dev/null | sort -rn | head -25
```

### 3.4 Technical Debt Markers

```
rg -i "TODO|FIXME|HACK|XXX|BUG|WORKAROUND|TEMPORARY|KLUDGE|DEPRECATED" --sort path -C 1 -g '!*node_modules*' -g '!*dist*' -g '!*.lock' 2>/dev/null
```

```
rg -ic "TODO|FIXME|HACK|XXX|BUG" --sort path -g '!*node_modules*' -g '!*dist*' 2>/dev/null | sort -t: -k2 -rn | head -15
```

### 3.5 Dead Code Indicators

```
rg -n "^(\s*//|\s*#)\s*(import|const|let|var|function|class|def |if |for |return)" -t py -t js -t ts -g '!*node_modules*' 2>/dev/null | head -25
```

```
rg -n "(console\.(log|debug|info|warn)|print\(|puts |binding\.pry|debugger;|breakpoint\(\))" -t py -t js -t ts -t rb -g '!*test*' -g '!*spec*' -g '!*node_modules*' 2>/dev/null | head -25
```

### 3.6 DRY Violations (duplicate patterns)

```
rg -c "\.addEventListener\(|\.removeEventListener\(" -t js -t ts 2>/dev/null | sort -t: -k2 -rn | head -10
rg -c "fetch\(|axios\.|\.get\(|\.post\(" -t js -t ts -g '!*test*' -g '!*node_modules*' 2>/dev/null | sort -t: -k2 -rn | head -10
```

---

## PHASE 4 — Architecture Mapping

### 4.1 Import / Dependency Graph

```
rg "^import |^from .* import|^const.*require\(|^import\s+\{|^import\s+type" --sort path -g '!*node_modules*' -g '!*dist*' 2>/dev/null | head -100
```

### 4.2 Exports / Public API Surface

```
rg "^export\s+(default\s+)?(class|function|const|interface|type|enum)" -t js -t ts --sort path 2>/dev/null | head -50
```

### 4.3 Environment Variables (config surface area)

```
rg "(process\.env\.|os\.environ|os\.getenv|ENV\[|dotenv|Deno\.env|std::env)" --sort path -g '!*node_modules*' -g '!*dist*' 2>/dev/null | head -40
```

### 4.4 Error Handling Patterns

```
rg -c "(try\s|catch\s|except\s|\.catch\(|\.on\(['\"]error|panic\(|unwrap\(\))" --sort path -t py -t js -t ts -t go -t rust 2>/dev/null | sort -t: -k2 -rn | head -20
```

### 4.5 Circular Dependency Candidates

Identify the most-imported modules:

```
rg -c "from\s+['\"]|require\s*\(['\"]|import\s+.*from\s+['\"]" --sort path -t py -t js -t ts 2>/dev/null | sort -t: -k2 -rn | head -15
```

---

## PHASE 5 — Performance Patterns

### 5.1 N+1 / Unbounded Queries

```
rg -i "(\.find\(|\.findAll|\.filter\(|\.select\(|SELECT|\.query\()" -t py -t js -t ts -g '!*test*' -g '!*migration*' 2>/dev/null | head -30
rg -i "LIMIT|\.limit\(|\.take\(" -t py -t js -t ts 2>/dev/null | wc -l
```

### 5.2 Sync Blocking in Async

```
rg -i "(readFileSync|writeFileSync|execSync|spawnSync|fs\.\w+Sync)" -t js -t ts -g '!*test*' -C 1 2>/dev/null
rg -i "time\.sleep|sleep\(" -t py -C 1 2>/dev/null
```

### 5.3 Memory Leak Indicators

```
rg -i "(addEventListener|setInterval|setTimeout)" -t js -t ts -g '!*test*' 2>/dev/null | head -20
rg -i "(removeEventListener|clearInterval|clearTimeout)" -t js -t ts -g '!*test*' 2>/dev/null | wc -l
```

### 5.4 Import Bloat

```
rg "import\s+\w+\s+from\s+['\"]lodash['\"]|require\(['\"]lodash['\"]|from lodash import" -t js -t ts -t py 2>/dev/null
rg "import\s+\*\s+as" -t js -t ts 2>/dev/null | head -15
```

---

## PHASE 6 — Testing Assessment

### 6.1 Test Coverage Mapping

```
echo "=== Test file count ===" && rg -l "\.(test|spec)\.(js|ts|jsx|tsx|py|rb|go)" 2>/dev/null | wc -l
echo "=== Source file count ===" && rg --files -t py -t js -t ts -t go -t rust -t java -t rb -g '!*test*' -g '!*spec*' -g '!*__tests__*' -g '!*node_modules*' -g '!*dist*' 2>/dev/null | wc -l
```

### 6.2 Assertion Density

```
rg -c "(expect\(|assert[A-Z]|assert |\.should\.|\.to\.|\.toBe|\.toEqual|\.toHave|\.toThrow|\.rejects|testing\.T)" --sort path 2>/dev/null | sort -t: -k2 -rn | head -15
```

### 6.3 Test Block Counts

```
rg -c "^(describe|it|test|def test_|func Test)\s*[\(\"]" --sort path 2>/dev/null | sort -t: -k2 -rn | head -15
```

### 6.4 CI/CD Detection

```
ls -la .github/workflows/ .gitlab-ci.yml Jenkinsfile .circleci/ .travis.yml bitbucket-pipelines.yml 2>/dev/null
cat .github/workflows/*.yml 2>/dev/null | head -60
```

---

## PHASE 7 — Static Analysis (run every applicable tool)

Detect which tools are available and run them:

```
# JavaScript / TypeScript
if [ -f package.json ]; then
  echo "=== npm audit ===" && npm audit --audit-level=moderate 2>/dev/null | tail -40
  echo "=== npx tsc --noEmit ===" && npx tsc --noEmit 2>/dev/null | tail -40
  echo "=== npx eslint ===" && npx eslint . --format compact 2>/dev/null | tail -40
  echo "=== npx depcheck ===" && npx depcheck 2>/dev/null | tail -30
fi
```

```
# Python
if [ -f requirements.txt ] || [ -f pyproject.toml ] || [ -f setup.py ]; then
  echo "=== ruff check ===" && ruff check . 2>/dev/null | tail -40
  echo "=== mypy ===" && mypy . 2>/dev/null | tail -40
  echo "=== bandit ===" && bandit -r . -ll 2>/dev/null | tail -40
  echo "=== pip audit ===" && pip audit 2>/dev/null | tail -30
fi
```

```
# Go
if [ -f go.mod ]; then
  echo "=== go vet ===" && go vet ./... 2>/dev/null | tail -30
  echo "=== staticcheck ===" && staticcheck ./... 2>/dev/null | tail -30
fi
```

```
# Rust
if [ -f Cargo.toml ]; then
  echo "=== cargo clippy ===" && cargo clippy 2>&1 | tail -40
  echo "=== cargo audit ===" && cargo audit 2>/dev/null | tail -30
fi
```

---

## PHASE 8 — Deep Read Critical Files

After the sweeps above, identify the **top 10–15 hotspot files** — files that appear across multiple categories (high complexity + security findings + missing tests + high import count). Read each one fully using the `read` tool and analyze for:

- Business logic correctness
- Edge case handling
- Input validation completeness
- Error handling robustness
- Naming clarity and documentation
- Separation of concerns violations

---

## PHASE 9 — Report Generation

After completing ALL phases above, produce a single comprehensive markdown report. Write it to `.opencode/plans/audit-report.md` using the write tool.

Use this exact structure:

```markdown
# Code Audit Report

**Project**: [name from manifest or directory]
**Date**: [today's date]
**Auditor**: OpenCode (automated static analysis)
**Scope**: Full codebase — 6-dimension audit

---

## Executive Summary

| Metric | Value |
|---|---|
| **Overall Health Score** | [1–10, where 10 = excellent] |
| **Critical Issues** | [count] |
| **High Priority Issues** | [count] |
| **Medium Priority Issues** | [count] |
| **Low Priority Issues** | [count] |

### Top 3 Priorities
1. **[Issue]** — [Category] — [one-line description]
2. **[Issue]** — [Category] — [one-line description]
3. **[Issue]** — [Category] — [one-line description]

### What's Working Well
- [Positive finding 1]
- [Positive finding 2]
- [Positive finding 3]

---

## Findings

### 🔴 Critical

#### [C-01] [Brief Title]
- **File**: `path/to/file.ext:line`
- **Category**: [Security | Performance | Architecture | Quality | Testing | Maintainability]
- **Issue**: [Specific problem description]
- **Impact**: [What can go wrong — security breach, data loss, outage, etc.]
- **Evidence**:
  ```
  [Exact code snippet, 3-5 lines max]
  ```
- **Recommendation**: [Specific actionable fix with example code if helpful]
- **Effort**: [Small (<30 min) | Medium (1-4 hrs) | Large (1+ days)]

### 🟠 High Priority
[Same structure as Critical, using H-01, H-02, etc.]

### 🟡 Medium Priority
[Same structure, using M-01, M-02, etc.]

### 🟢 Low Priority / Improvements
[Same structure, using L-01, L-02, etc.]

---

## Category Deep Dives

### 1. Architecture & Design
[Narrative summary with file:line references cross-linked to finding IDs]

### 2. Code Quality
[Narrative summary]

### 3. Security
[Narrative summary]

### 4. Performance
[Narrative summary]

### 5. Testing
[Narrative summary]

### 6. Maintainability
[Narrative summary]

---

## Prioritized Action Plan

### Quick Wins (< 1 day each)
- [ ] **[ID]** `file:line` — [Action verb] [specific task]

### Medium-term (1–5 days each)
- [ ] **[ID]** [Description of work]

### Strategic Initiatives (> 5 days)
- [ ] [Architectural improvement description]

---

## Metrics Dashboard

| Metric | Value |
|---|---|
| Files Analyzed | [N] |
| Total Lines of Code | [N] |
| Languages Detected | [list] |
| Test-to-Source File Ratio | [N:N] |
| Complexity Hotspots (files) | [N] |
| Security Findings | 🔴 [N]  🟠 [N]  🟡 [N]  🟢 [N] |
| TODO / FIXME / HACK Count | [N] / [N] / [N] |
| Direct Dependencies | [N] |
| Avg File Length (LOC) | [N] |
| Longest File | `file` ([N] lines) |
```

---

## Execution Rules

1. **Thoroughness over speed** — This is a DEEP audit. Run every search pattern. Do not skip phases.
2. **Traceable evidence** — Every finding MUST include `file:line`. No vague claims. If you can't pinpoint it, it's not a finding.
3. **Cross-reference** — Map relationships across categories. A complex file that also lacks tests AND has unvalidated SQL is three related findings.
4. **Positive findings** — Call out things done well. Good patterns, solid coverage, clean architecture.
5. **Run all applicable static analysis tools** — Check what's available and run everything.
6. **Realistic effort estimates** — "Small" = < 30 min including testing. Don't underestimate refactoring.
7. **READ-ONLY** — Do not modify, create, or delete any project files EXCEPT writing the report to `.opencode/plans/audit-report.md`.
8. **Prioritize by risk** — Critical = exploitable security flaw or data loss. High = production bugs or major debt. Medium = quality concerns. Low = polish.
9. **Adapt to language** — Not all patterns apply to all languages. Skip irrelevant checks but document why.
10. **Surface the non-obvious** — Anyone can spot a TODO. Find the implicit coupling, the missing validation, the edge case that will bite in production.
