#!/bin/bash
# Development script to run all checks: linting, formatting, and type checking
# Part of the uv + ruff + ty workflow

set -e

echo "🔍 Running Ruff linter..."
uv run ruff check nba_vault tests

echo "🎨 Checking code formatting with Ruff..."
uv run ruff format --check nba_vault tests

echo "🔬 Running type checker Ty..."
uv run ty check

echo "💾 Checking SQL files with SQLFluff..."
uv run sqlfluff lint migrations --dialect sqlite
uv run sqlfluff lint duckdb/views --dialect duckdb

echo "✅ All checks passed!"
