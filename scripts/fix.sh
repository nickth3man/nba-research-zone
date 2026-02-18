#!/bin/bash
# Development script to auto-fix linting and formatting issues
# Part of the uv + ruff + ty workflow

set -e

echo "🔧 Auto-fixing Ruff linting issues..."
uv run ruff check --fix nba_vault tests

echo "🎨 Formatting code with Ruff..."
uv run ruff format nba_vault tests

echo "💾 Formatting SQL files with SQLFluff..."
echo "  → Formatting migrations (SQLite)..."
uv run sqlfluff fix migrations --dialect sqlite --force
echo "  → Formatting DuckDB views..."
uv run sqlfluff fix duckdb/views --dialect duckdb --force

echo "✅ Code and SQL fixed and formatted!"
