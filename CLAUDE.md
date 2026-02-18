
## Project Overview

NBA Vault is a comprehensive historical NBA database system using a **dual-engine architecture**: **SQLite** as the source of truth and **DuckDB** as the analytical engine. The system covers 78+ seasons of NBA/ABA/BAA history (1946-present) with incremental updates and optimized analytical queries.

## Core Architecture

### Ingestion Pipeline Pattern

All data ingestion follows a consistent three-stage pipeline implemented by `BaseIngestor`:

1. **fetch()**: Retrieve raw data from external APIs (with rate limiting and caching)
2. **validate()**: Validate data using Pydantic models
3. **upsert()**: Insert or update in SQLite database

New ingestors are registered using the `@register_ingestor` decorator and automatically discovered via the registry pattern. The `ingest()` method orchestrates the full pipeline with built-in retry logic and error handling.

### Dual-Database Design

- **SQLite** (`nba.sqlite`): Primary storage with optimized PRAGMAs (WAL mode, 128MB cache, 16KB pages, FK enforcement)
- **DuckDB** (`nba.duckdb`): Analytical engine with pre-built views, created by attaching SQLite as a read-only source

The DuckDB builder automatically creates views from `duckdb/views/*.sql` files, which use `sqlite_db` prefix to reference SQLite tables.

### Database Schema

The schema uses a **franchise vs team** separation:
- `franchise`: Historical continuity (team may relocate/rename)
- `team`: Season-specific records (links to franchise via foreign key)

Key design pattern: `data_availability_flags` bitmask field tracks which data types are loaded (box scores, play-by-play, shot charts, etc.) - see `data_availability_flag_def` table for bit definitions.

### Migration System

Uses **yoyo-migrations** with SQL files in `migrations/` directory. Migrations are versioned and support rollback. Always use idempotent SQL patterns (`CREATE TABLE IF NOT EXISTS`, `INSERT OR REPLACE`, etc.).

## Essential Commands

### Development Workflow

```bash
# Install dependencies
uv sync --group dev

# Run all quality checks (Python + SQL)
./scripts/check.sh     # macOS/Linux
./scripts/check.ps1    # Windows

# Auto-fix all formatting issues
./scripts/fix.sh       # macOS/Linux
./scripts/fix.ps1      # Windows

# Individual tool commands
uv run ruff check nba_vault tests           # Lint Python
uv run ruff format nba_vault tests          # Format Python
uv run ty check                             # Type check
uv run sqlfluff lint migrations --dialect sqlite
uv run sqlfluff lint duckdb/views --dialect duckdb
```

### Database Operations

```bash
# Initialize database (run migrations)
uv run nba-vault init

# Rollback migrations
uv run nba-vault migrate --rollback --steps 1

# Build DuckDB analytical database
uv run nba-vault export --format duckdb

# Check database status
uv run nba-vault status
```

### Testing

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_schema.py

# Run with coverage
uv run pytest --cov=nba_vault --cov-report=html

# Run single test
uv run pytest tests/test_schema.py::test_database_initialization
```

## Key Code Patterns

### Creating New Ingestors

```python
from nba_vault.ingestion import BaseIngestor, register_ingestor
from nba_vault.models import YourModelCreate

@register_ingestor
class YourIngestor(BaseIngestor):
    entity_type = "your_entity"

    def fetch(self, entity_id: str, **kwargs):
        # Fetch from API with rate limiting
        self.rate_limiter.acquire()
        return api_call(entity_id)

    def validate(self, raw: dict):
        # Validate with Pydantic
        return YourModelCreate(**raw)

    def upsert(self, model: YourModelCreate, conn):
        # Insert to database
        conn.execute("INSERT OR REPLACE INTO ...")
```

### Database Connections

Always use `get_db_connection()` from `nba_vault.schema.connection` to get connections with optimized PRAGMAs. Connections use `row_factory = sqlite3.Row` for dict-like row access.

### Structured Logging

Use `structlog.get_logger(__name__)` for structured logging with contextual information. The logging configuration supports both JSON (production) and console (development) formats.

### Configuration

Access settings via `get_settings()` from `nba_vault.utils.config`. Settings are cached with `@lru_cache` and loaded from environment variables or `.env` file.

## Important File Locations

- **Migrations**: `migrations/*.sql` - Database schema changes (SQLite dialect)
- **DuckDB Views**: `duckdb/views/v_*.sql` - Analytical views (DuckDB dialect)
- **Models**: `nba_vault/models/*.py` - Pydantic validation models
- **Ingestors**: `nba_vault/ingestion/*.py` - Data ingestion implementations
- **Configuration**: `.env`, `.sqlfluff`, `pyproject.toml`

## SQL Dialects

- **SQLite**: Used for migrations and primary database schema
- **DuckDB**: Used for analytical views (attached to SQLite via `sqlite_db` prefix)

Always specify the correct dialect when running SQLFluff commands.

## Pre-commit Hooks

Pre-commit hooks automatically run:
- Ruff linting with auto-fix
- Ruff formatting
- SQLFluff linting and formatting for SQL files

Install with: `uv run pre-commit install`
