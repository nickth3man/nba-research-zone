# PowerShell script for Windows - auto-fixes issues
# Part of the uv + ruff + ty workflow

Write-Host "🔧 Auto-fixing Ruff linting issues..." -ForegroundColor Cyan
uv run ruff check --fix nba_vault tests

Write-Host "🎨 Formatting code with Ruff..." -ForegroundColor Cyan
uv run ruff format nba_vault tests

Write-Host "💾 Formatting SQL files with SQLFluff..." -ForegroundColor Cyan
Write-Host "  → Formatting migrations (SQLite)..." -ForegroundColor Gray
uv run sqlfluff fix migrations --dialect sqlite --force
Write-Host "  → Formatting DuckDB views..." -ForegroundColor Gray
uv run sqlfluff fix duckdb/views --dialect duckdb --force

Write-Host "✅ Code and SQL fixed and formatted!" -ForegroundColor Green
