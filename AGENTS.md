
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

# Run new ingestor tests
uv run pytest tests/test_new_ingestors.py
```

## Key Code Patterns

### Creating New Ingestors

All ingestors follow the three-stage `fetch() → validate() → upsert()` pattern. The `ingest()` method in `BaseIngestor` orchestrates this pipeline with built-in retry logic and error handling.

```python
from nba_vault.ingestion import BaseIngestor, register_ingestor
from nba_vault.models import YourModelCreate

@register_ingestor
class YourIngestor(BaseIngestor):
    entity_type = "your_entity"

    def __init__(self, cache=None, rate_limiter=None):
        super().__init__(cache, rate_limiter)
        # Initialize API clients or scrapers here

    def fetch(self, entity_id: str, **kwargs):
        # Retrieve raw data from external APIs
        # Uses self.rate_limiter.acquire() for rate limiting
        # Returns dict with raw data
        return api_call(entity_id)

    def validate(self, raw: dict):
        # Validate using Pydantic models from nba_vault.models
        # Returns list of validated model instances
        return [YourModelCreate(**item) for item in raw["items"]]

    def upsert(self, model: YourModelCreate, conn):
        # Insert or update in database
        # Check if exists, then insert or update
        # Log to ingestion_audit table
        # Return rows_affected count
```

### Ingestor Discovery and Usage

Ingestors are auto-discovered via the registry:

```python
from nba_vault.ingestion import create_ingestor, get_ingestor, list_ingestors

# Get all registered ingestors
ingestor_types = list_ingestors()  # ["players", "player_tracking", "lineups", ...]

# Create ingestor instance
ingestor = create_ingestor("player_tracking")

# Use the ingestor
from nba_vault.schema.connection import get_db_connection
conn = get_db_connection()
result = ingestor.ingest("2544", conn, season="2023-24")
# result = {"status": "SUCCESS"|"FAILED", "entity_id": ..., "rows_affected": ...}
```

### Data Availability Handling

Many data types have historical limitations. Always validate season availability:

```python
season_year = int(season.split("-")[0])
if season_year < 2013:
    raise ValueError("Player tracking data only available from 2013-14 onwards")
```

### Available Ingestors

The system includes 7 ingestors:

- `players`: Basic player data from Basketball Reference (1946-present)
- `player_tracking`: Movement metrics from NBA.com API (2013-14+)
- `lineups`: Lineup combinations from NBA.com API
- `team_other_stats`: Paint points, fast break, etc. from NBA.com
- `team_advanced_stats`: Off/def rating, pace, four factors from NBA.com
- `injuries`: Injury reports from ESPN/Rotowire (web scraping)
- `contracts`: Contract data from RealGM/Spotrac (web scraping)

### Database Connections

Always use `get_db_connection()` from `nba_vault.schema.connection` to get connections with optimized PRAGMAs. Connections use `row_factory = sqlite3.Row` for dict-like row access.

### Structured Logging

Use `structlog.get_logger(__name__)` for structured logging with contextual information. The logging configuration supports both JSON (production) and console (development) formats.

### Configuration

Access settings via `get_settings()` from `nba_vault.utils.config`. Settings are cached with `@lru_cache` and loaded from environment variables or `.env` file.

## Important File Locations

### Core Architecture Files

- **Migrations**: `migrations/*.sql` - Database schema changes (SQLite dialect, yoyo-migrations)
- **DuckDB Views**: `duckdb/views/v_*.sql` - Analytical views (DuckDB dialect, auto-loaded by builder)
- **Models**: `nba_vault/models/*.py` - Pydantic validation models
- **Ingestors**: `nba_vault/ingestion/*.py` - Data ingestion implementations
- **API Clients**: `nba_vault/ingestion/nba_stats_client.py`, `basketball_reference.py`

### Configuration

- **Environment**: `.env` - Local environment variables (not in repo)
- **Project**: `pyproject.toml` - Dependencies, tool config (ruff, ty, sqlfluff)
- **SQL Linting**: `.sqlfluff` - SQLFluff configuration
- **Pre-commit**: `.pre-commit-config.yaml` - Pre-commit hooks (ruff)

### Entry Points

- **CLI**: `nba_vault/cli.py` - Command-line interface (typer-based)
- **Connection**: `nba_vault/schema/connection.py` - Database connection management
- **Config**: `nba_vault/utils/config.py` - Settings management (pydantic-settings)
- **Migrations**: `nba_vault/schema/migrations.py` - Migration runner (yoyo-migrations)

## SQL Dialects

- **SQLite**: Used for migrations and primary database schema
- **DuckDB**: Used for analytical views (attached to SQLite via `sqlite_db` prefix)

Always specify the correct dialect when running SQLFluff commands.

## Data Ingestion CLI Commands

### Player Data

```bash
# Ingest players from Basketball Reference
nba-vault ingest-players --season-end-year 2024
nba-vault ingest-players --player-id jamesle01
```

### Advanced Stats (NBA.com API)

```bash
# Player tracking data (2013-14+)
nba-vault ingest-tracking --player-id 2544 --season 2023-24

# Lineup combinations
nba-vault ingest-lineups --scope league --season 2023-24

# Team other stats (paint points, fast break)
nba-vault ingest-team-other-stats --game-id 0022300001

# Advanced team stats (off/def rating, pace)
nba-vault ingest-team-advanced-stats --scope league --season 2023-24
```

### Web Scraping Sources

```bash
# Injury reports (ESPN/Rotowire)
nba-vault ingest-injuries --source espn

# Contract data (RealGM/Spotrac)
nba-vault ingest-contracts --source realgm
```

## Season Format Conventions

- **NBA.com API**: Uses "YYYY-YY" format (e.g., "2023-24")
- **Season IDs**: Integer year of season start (e.g., 2023)
- **Game IDs**: 10-character strings (e.g., "0022300001")
- **Team IDs**: Integer IDs from NBA.com (e.g., 1610612747 for Lakers)

## Testing Patterns

### Test Fixtures

```python
import pytest
from nba_vault.schema.connection import get_db_connection

@pytest.fixture
def db_connection():
    conn = get_db_connection()
    yield conn
    conn.close()
```

### Mocking API Responses

```python
from unittest.mock import patch

@patch("nba_vault.ingestion.nba_stats_client.NBAStatsClient._make_request")
def test_something(mock_request):
    mock_request.return_value = {"data": [...]}
    # Test code here
```

### Test Location Pattern

- Unit tests: `tests/test_<module>.py`
- Integration tests: `tests/test_ingestion*.py`
- New ingestors: `tests/test_new_ingestors.py`

## Pre-commit Hooks

Pre-commit hooks automatically run:

- Ruff linting with auto-fix
- Ruff formatting
- SQLFluff linting and formatting for SQL files

Install with: `uv run pre-commit install`

## File Synchronization

AGENTS.md and CLAUDE.md are identical files serving different AI agents.
Whenever you modify one, you must immediately update the other with the
exact same contents. No diff, no summarization — a byte-for-byte copy.
