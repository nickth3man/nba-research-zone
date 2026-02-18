# PowerShell script for Windows - runs all checks
# Part of the uv + ruff + ty workflow

Write-Host "🔍 Running Ruff linter..." -ForegroundColor Cyan
uv run ruff check nba_vault tests

Write-Host "🎨 Checking code formatting with Ruff..." -ForegroundColor Cyan
uv run ruff format --check nba_vault tests

Write-Host "🔬 Running type checker Ty..." -ForegroundColor Cyan
uv run ty check

Write-Host "💾 Checking SQL files with SQLFluff..." -ForegroundColor Cyan
Write-Host "  → Checking migrations (SQLite)..." -ForegroundColor Gray
uv run sqlfluff lint migrations --dialect sqlite
Write-Host "  → Checking DuckDB views..." -ForegroundColor Gray
uv run sqlfluff lint duckdb/views --dialect duckdb

Write-Host "🧪 Running tests with coverage..." -ForegroundColor Cyan
uv run pytest --cov=nba_vault --cov-report=term-missing

Write-Host "✅ All checks passed!" -ForegroundColor Green
