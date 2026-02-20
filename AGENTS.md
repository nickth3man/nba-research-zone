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
# Run all tests (fast — no coverage overhead)
uv run pytest

# Run in parallel using all CPU cores (pytest-xdist, ~2-3x faster)
uv run pytest -n auto

# Run with coverage (opt-in; also runs automatically via check.sh / check.ps1)
uv run pytest --cov=nba_vault --cov-report=term-missing

# Run specific test file
uv run pytest tests/test_schema.py

# Run single test
uv run pytest tests/test_schema.py::test_database_initialization

# Run new ingestor tests
uv run pytest tests/test_new_ingestors.py
```

> **Coverage is intentionally not in `addopts`** so normal `uv run pytest` runs are fast.
> The check scripts (`check.sh` / `check.ps1`) always pass `--cov` for full quality checks.

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

The system includes **24 ingestors** across 9 domains. See `DATA_SOURCING.md` for the
full source/era-gate reference.

**Reference / Seed Data**
- `players`: Basic player data from Basketball Reference (1946-present)
- `franchises`: Franchise history from NBA.com (all eras)
- `seasons`: Season metadata from NBA.com (all eras)
- `player_bio`: Player biographical details from NBA.com (all eras)
- `coaches`: Coaching staff from NBA.com `commonTeamRoster` (all eras)

**Games & Officials**
- `game_schedule`: Game schedule from NBA.com `leagueGameLog` (all eras)
- `game_officials`: Game referees from NBA.com `boxScoreSummaryV2` (all eras)

**Box Scores**
- `box_scores_traditional`: Traditional box scores from NBA.com (1996-97+)
- `box_scores_advanced`: Advanced box scores from NBA.com (1996-97+)
- `box_scores_hustle`: Hustle-stats box scores from NBA.com (**2015-16+**)
- `pre_modern_box_scores`: Pre-1996 player box scores from eoinamoore Kaggle dataset (CC0, 1947+)

**Play-by-Play & Shot Charts**
- `play_by_play`: Play-by-play event log from NBA.com (1996-97+)
- `shot_chart`: Shot chart (FGA locations) from NBA.com (1996-97+)
- `shufinskiy_pbp`: Pre-assembled PBP + shot charts from shufinskiy/nba_data (Apache-2.0, **1996-97+**)

**Player Stats & Tracking**
- `player_season_stats`: Per-season stats from NBA.com `playerCareerStats` (all eras)
- `player_tracking`: Movement metrics from NBA.com `playerDashPtStats` (**2013-14+**)

**Team Stats**
- `lineups`: Lineup combinations from NBA.com (1996-97+)
- `team_other_stats`: Paint points, fast break, etc. from NBA.com (1996-97+)
- `team_advanced_stats`: Off/def rating, pace, four factors from NBA.com (1996-97+)

**Draft**
- `draft`: Draft history from NBA.com `draftHistory` (1947+)
- `draft_combine`: Combine measurements + drills from NBA.com (**2000+**)

**Awards & Injuries**
- `awards`: Career awards (MVP, All-Star, All-NBA…) from NBA.com (all eras)
- `injuries`: Current-season injury reports from ESPN/Rotowire (web scraping)

**Open-Source Analytics (bulk download)**
- `elo_ratings`: ELO ratings per team per game from Neil-Paine-1/NBA-elo (MIT, **1946-present**)
- `raptor_ratings`: RAPTOR player metrics from FiveThirtyEight (CC BY 4.0, **1976-present**)

**Intentionally excluded (stub)**
- `contracts`: All pipeline methods raise `NotImplementedError` — salary data excluded per PRD §3

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
- **Entities models**: `nba_vault/models/entities.py` - All Pydantic v2 Create models
- **Ingestors**: `nba_vault/ingestion/*.py` - Data ingestion implementations
- **API Clients**: `nba_vault/ingestion/nba_stats_client.py`, `basketball_reference.py`
- **Data sourcing reference**: `DATA_SOURCING.md` - All sources, era gates, CLI commands

### Configuration

- **Environment**: `.env` - Local environment variables (not in repo)
- **Project**: `pyproject.toml` - Dependencies, tool config (ruff, ty, sqlfluff)
- **SQL Linting**: `.sqlfluff` - SQLFluff configuration
- **Pre-commit**: `.pre-commit-config.yaml` - Pre-commit hooks (ruff); note the leading dot is required
- **Python version**: `.python-version` - Pins Python 3.12.8 for uv
- **Editor config**: `.editorconfig` - Consistent editor settings (indent, line endings, etc.)
- **CI**: `.github/workflows/ci.yml` - GitHub Actions CI (lint + test jobs)

### Entry Points

- **CLI**: `nba_vault/cli/` - Command-line interface (typer-based, decomposed into sub-modules)
  - `admin.py` — `init`, `migrate`, `status`, `validate`
  - `ingestion.py` — `ingest-players`
  - `advanced_stats.py` — `ingest-tracking`, `ingest-lineups`, `ingest-team-*`
  - `game_data.py` — `ingest-seasons`, `ingest-franchises`, `ingest-schedule`, `ingest-officials`, `ingest-box-scores`, `ingest-box-scores-advanced`, `ingest-box-scores-hustle`, `ingest-pbp`, `ingest-shot-charts`, `ingest-player-bio`, `ingest-coaches`, `ingest-draft`, `ingest-draft-combine`, `ingest-awards`, `ingest-season-stats`
  - `scrapers.py` — `ingest-injuries`, `ingest-contracts`
  - `export.py` — `export`
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
nba-vault ingestion ingest-players --season-end-year 2024
nba-vault ingestion ingest-players --player-id jamesle01
```

### Advanced Stats (NBA.com API)

```bash
# Player tracking data (2013-14+)
nba-vault advanced-stats ingest-tracking --player-id 2544 --season 2023-24

# Lineup combinations
nba-vault advanced-stats ingest-lineups --scope league --season 2023-24

# Team other stats (paint points, fast break)
nba-vault advanced-stats ingest-team-other-stats --game-id 0022300001

# Advanced team stats (off/def rating, pace)
nba-vault advanced-stats ingest-team-advanced-stats --scope league --season 2023-24
```

### Game Data (NBA.com API)

```bash
# Season metadata
nba-vault game-data ingest-seasons --season 2023-24

# Franchise history
nba-vault game-data ingest-franchises

# Game schedule
nba-vault game-data ingest-schedule --season 2023-24

# Officials for a game
nba-vault game-data ingest-officials --game-id 0022300001

# Box scores
nba-vault game-data ingest-box-scores --game-id 0022300001
nba-vault game-data ingest-box-scores-advanced --game-id 0022300001
nba-vault game-data ingest-box-scores-hustle --game-id 0022300001   # 2015-16+

# Play-by-play
nba-vault game-data ingest-pbp --game-id 0022300001

# Shot charts
nba-vault game-data ingest-shot-charts --player-id 2544 --season 2023-24
nba-vault game-data ingest-shot-charts --game-id 0022300001

# Player bio + coaches
nba-vault game-data ingest-player-bio --player-id 2544
nba-vault game-data ingest-coaches --team-id 1610612747 --season 2023-24

# Draft
nba-vault game-data ingest-draft                  # all years
nba-vault game-data ingest-draft --year 2024
nba-vault game-data ingest-draft-combine --year 2024   # 2000+

# Awards + season stats
nba-vault game-data ingest-awards --player-id 2544
nba-vault game-data ingest-season-stats --player-id 2544 --per-mode Totals
```

### Web Scraping Sources

```bash
# Injury reports (ESPN/Rotowire)
nba-vault scrapers ingest-injuries --source espn

# Contract data (stub — raises NotImplementedError; excluded per PRD §3)
nba-vault scrapers ingest-contracts --source realgm
```

## Season Format Conventions

- **NBA.com API**: Uses "YYYY-YY" format (e.g., "2023-24")
- **Season IDs**: Integer year of season start (e.g., 2023)
- **Game IDs**: 10-character strings (e.g., "0022300001")
- **Team IDs**: Integer IDs from NBA.com (e.g., 1610612747 for Lakers)

## Testing Patterns

### Test Fixtures

`tests/conftest.py` defines three shared fixtures:

```python
# temp_db_path — private per-test DB file; use when a test needs to run
# migrations itself (e.g. testing run_migrations() or rollback_migration())
@pytest.fixture
def temp_db_path():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        yield Path(f.name)

# migrated_db_path — session-scoped: migrations run ONCE per pytest session.
# tmp_path_factory is xdist-safe; each worker gets its own temp directory.
@pytest.fixture(scope="session")
def migrated_db_path(tmp_path_factory):
    db = tmp_path_factory.mktemp("session_db") / "test.db"
    run_migrations(db)
    return db

# db_connection — function-scoped fresh connection to the session-migrated DB.
# Do NOT wrap in BEGIN/ROLLBACK — ingestors call conn.execute("BEGIN") internally
# and SQLite raises if BEGIN is nested. Tests use distinct entity IDs so
# accumulated data across tests is safe.
@pytest.fixture
def db_connection(migrated_db_path):
    conn = sqlite3.connect(str(migrated_db_path))
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()
```

**Rule**: use `db_connection` for tests that need an already-migrated schema.
Use `temp_db_path` directly only for tests that exercise the migration machinery itself.

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

## DX Toolchain Notes

### Python Version

The project targets **Python 3.12.8**, pinned via `.python-version`. The `pyproject.toml` uses `requires-python = ">=3.12,<3.13"` to prevent cross-version resolution issues with `uv`.

### Ruff Configuration

Ruff is configured in `pyproject.toml` with the following rule sets enabled beyond defaults:

- `"S"` — bandit security checks
- `"G"` — logging format checks
- `"SIM"` — simplification suggestions

The `target-version = "py312"` is set to match the Python version pin.

### Type Checker: ty

The project uses `ty` (by Astral) for type checking, version `>=0.0.1` (latest: 0.0.17 as of Feb 2026). Note: `ty>=0.2.0` does not exist — use `>=0.0.1`. Pre-existing `unresolved-import` errors for `create_ingestor` in `cli.py` are expected (the function is resolved at runtime via the registry pattern) and are not regressions.

### Pre-commit Config

The pre-commit config file is `.pre-commit-config.yaml` (with a leading dot). An older version without the dot (`pre-commit-config.yaml`) was renamed. Always use the dotfile name.

### pytest-xdist

`pytest-xdist>=3.5.0` is installed as a dev dependency. Run parallel tests with:

```bash
uv run pytest -n auto           # use all physical CPU cores
uv run pytest -n auto --dist=worksteal  # better for uneven test durations
```

The `migrated_db_path` session fixture is xdist-safe: `tmp_path_factory` creates a
unique temp directory per worker, so each worker gets its own migrated SQLite file.

### CI Workflow

GitHub Actions CI is defined at `.github/workflows/ci.yml` with two jobs:

- **lint**: Runs ruff check, ruff format --check, ty check, sqlfluff lint (both dialects)
- **test**: Runs pytest with coverage, uploads to Codecov

## File Synchronization

AGENTS.md and CLAUDE.md are identical files serving different AI agents.
Whenever you modify one, you must immediately update the other with the
exact same contents. No diff, no summarization — a byte-for-byte copy.
