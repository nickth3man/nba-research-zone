# NBA Vault

A comprehensive, open-source historical NBA database built with SQLite and DuckDB. NBA Vault provides 78+ seasons of NBA/ABA/BAA history (1946–present) in a query-optimized dual-engine architecture.

## Features

- **Complete Historical Coverage**: All 78+ seasons of NBA/ABA/BAA history
- **Dual-Engine Architecture**: SQLite as the source of truth + DuckDB for fast analytics
- **7 Data Ingestors**: Players, tracking, lineups, team stats, injuries, and contracts
- **11 Analytical Views**: Pre-built DuckDB views for common research queries
- **Rate-Limited API Access**: Token bucket limiter with exponential backoff and jitter
- **Response Caching**: Filesystem cache to minimize redundant API calls
- **Incremental Updates**: Audit-tracked upserts — re-run safely at any time
- **Modern Tooling**: uv, ruff, ty, and SQLFluff for blazing-fast development

## Quick Start

### Prerequisites

- **uv**: Modern Python package manager

  ```bash
  # macOS/Linux
  curl -LsSf https://astral.sh/uv/install.sh | sh

  # Windows
  powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
  ```

### Installation

```bash
git clone https://github.com/yourusername/nba-vault.git
cd nba-vault

# Install runtime dependencies
uv sync

# Install with development tools (tests, linters, type checker)
uv sync --group dev
```

### Initialize the Database

```bash
# Copy and edit environment variables (optional — defaults work out of the box)
cp .env.example .env

# Create the database and run all migrations
uv run nba-vault admin init
```

### Ingest Your First Data

```bash
# Ingest player roster from Basketball Reference (2023-24 season)
uv run nba-vault ingestion ingest-players --season-end-year 2024

# Ingest tracking data for a specific player (LeBron James = 2544)
uv run nba-vault advanced-stats ingest-tracking --player-id 2544 --season 2023-24

# Ingest advanced team stats for the whole league
uv run nba-vault advanced-stats ingest-team-advanced-stats --scope league --season 2023-24

# Build DuckDB analytical database from SQLite
uv run nba-vault export export --format duckdb
```

### Check Status

```bash
uv run nba-vault admin status
```

---

## CLI Reference

The CLI is organized into five sub-command groups. Run `--help` on any command for full option docs.

### `admin` — Database administration

| Command | Description |
|---|---|
| `nba-vault admin init` | Create database and run all pending migrations |
| `nba-vault admin migrate` | Apply pending migrations |
| `nba-vault admin migrate --rollback --steps N` | Roll back N migrations |
| `nba-vault admin status` | Show table row counts and ingestion audit summary |

### `ingestion` — Core data ingestion

| Command | Description |
|---|---|
| `nba-vault ingestion ingest-players --season-end-year YEAR` | Ingest all players from a season (Basketball Reference) |
| `nba-vault ingestion ingest-players --player-id SLUG` | Ingest a single player by BR slug (e.g., `jamesle01`) |

### `advanced-stats` — NBA.com Stats API

| Command | Key Options | Notes |
|---|---|---|
| `ingest-tracking` | `--player-id`, `--team-id`, `--season` | Available from 2013-14 onwards |
| `ingest-lineups` | `--scope league\|team:<id>`, `--season` | 5-man lineup combinations |
| `ingest-team-other-stats` | `--game-id`, `--team-id`, `--season` | Paint pts, fast break, 2nd chance |
| `ingest-team-advanced-stats` | `--scope`, `--season`, `--measure-type` | Off/def rating, pace, four factors |

Prefix each command with `nba-vault advanced-stats`, e.g.:

```bash
uv run nba-vault advanced-stats ingest-tracking --player-id 2544 --season 2023-24
uv run nba-vault advanced-stats ingest-lineups --scope league --season 2023-24
uv run nba-vault advanced-stats ingest-team-other-stats --game-id 0022300001
uv run nba-vault advanced-stats ingest-team-advanced-stats --scope league --season 2023-24
```

### `scrapers` — Web scraping sources

| Command | Key Options | Sources |
|---|---|---|
| `ingest-injuries` | `--team ABBR`, `--source` | `espn`, `rotowire`, `nba` |
| `ingest-contracts` | `--team ABBR`, `--source` | `realgm`, `spotrac` |

```bash
uv run nba-vault scrapers ingest-injuries --source espn
uv run nba-vault scrapers ingest-contracts --source realgm
```

### `export` — Data export

| Command | Description |
|---|---|
| `nba-vault export export --format duckdb` | Build DuckDB analytical database (attaches SQLite, creates views) |

---

## Architecture

### Dual-Database Design

```
External APIs / Web Scrapers
         │
         ▼
  ┌─────────────┐    fetch → validate → upsert
  │  Ingestors  │─────────────────────────────►  nba.sqlite  (SQLite)
  └─────────────┘                                     │
                                                      │  DuckDB builder
                                                      ▼
                                               nba.duckdb  (DuckDB)
                                               └── 11 analytical views
```

- **SQLite** (`nba.sqlite`): Source of truth. WAL mode, 128 MB cache, 16 KB pages, foreign key enforcement.
- **DuckDB** (`nba.duckdb`): Analytical engine. Attaches SQLite as a read-only source and exposes pre-built views for fast aggregation queries.

### Ingestion Pipeline

Every ingestor inherits from `BaseIngestor` and implements three stages:

1. **`fetch(entity_id, **kwargs)`** — Call external API or scraper (rate-limited, cached)
2. **`validate(raw)`** — Parse and validate with Pydantic models
3. **`upsert(models, conn)`** — Insert or update in SQLite, log to `ingestion_audit`

The `ingest()` orchestrator handles retries with exponential backoff, catches validation errors, and returns a structured result dict:

```python
{"status": "SUCCESS" | "FAILED", "entity_id": ..., "rows_affected": ...}
```

### Rate Limiting & Retries

- **Token bucket** rate limiter: 8 req/min by default, configurable via `NBA_API_RATE_LIMIT`
- **Retry with backoff**: up to 5 attempts, 30 s base delay, doubles each attempt, ±20% jitter
- Both parameters are configurable in `.env` (see [Configuration](#configuration))

### Available Ingestors

| `entity_type` | Class | Source | Coverage |
|---|---|---|---|
| `players` | `PlayersIngestor` | Basketball Reference | 1946–present |
| `player_tracking` | `PlayerTrackingIngestor` | NBA.com Stats API | 2013-14+ |
| `lineups` | `LineupsIngestor` | NBA.com Stats API | 2013-14+ |
| `team_other_stats` | `TeamOtherStatsIngestor` | NBA.com Stats API | 2013-14+ |
| `team_advanced_stats` | `TeamAdvancedStatsIngestor` | NBA.com Stats API | 2013-14+ |
| `injuries` | `InjuryIngestor` | ESPN / Rotowire | Current season |
| `contracts` | `ContractIngestor` | RealGM / Spotrac | Current season |

### Analytical Views (DuckDB)

Views are loaded automatically from `duckdb/views/*.sql` when building the DuckDB database.

| View | Description |
|---|---|
| `franchise_timeline` | Franchise history across relocations and renames |
| `game_officials` | Officials assigned per game |
| `head_to_head_records` | All-time head-to-head records between franchises |
| `injury_impact` | Missed games and projected impact by player |
| `lineup_performance` | 5-man lineup efficiency metrics |
| `player_advanced_complete` | Complete advanced stats per player-season |
| `player_career_stats` | Career totals and per-game averages |
| `player_tracking_summary` | Aggregated tracking metrics (speed, touches, drives) |
| `season_leaders` | Statistical leaders per season and category |
| `shot_zone_agg` | Shot volume and efficiency by court zone |
| `team_efficiency_analysis` | Off/def ratings, pace, net rating over time |

---

## Configuration

Copy `.env.example` to `.env` and edit as needed. All settings have sensible defaults.

```bash
# ── Database ──────────────────────────────────────────
DB_PATH=nba.sqlite
DUCKDB_PATH=nba.duckdb

# ── NBA.com API ───────────────────────────────────────
NBA_API_RATE_LIMIT=8        # requests per minute
NBA_API_RETRY_ATTEMPTS=5    # max retry attempts
NBA_API_RETRY_DELAY=30      # base retry delay in seconds

# ── Response cache ────────────────────────────────────
CACHE_DIR=cache
CACHE_ENABLED=true

# ── Logging ───────────────────────────────────────────
LOG_LEVEL=INFO              # DEBUG | INFO | WARNING | ERROR | CRITICAL
LOG_FORMAT=json             # json | console
LOG_DIR=logs

# ── Historical backfill ───────────────────────────────
BACKFILL_WORKERS=1
BACKFILL_START_SEASON=1946
BACKFILL_END_SEASON=2024

# ── DuckDB ────────────────────────────────────────────
DUCKDB_MEMORY_LIMIT=4GB
DUCKDB_THREADS=4
```

---

## Development

### Workflow Scripts

```bash
# Run all quality checks (lint + format check + type check + SQL lint + tests with coverage)
./scripts/check.sh    # macOS/Linux
./scripts/check.ps1   # Windows

# Auto-fix all lint and format issues
./scripts/fix.sh      # macOS/Linux
./scripts/fix.ps1     # Windows
```

### Individual Tool Commands

```bash
uv run ruff check nba_vault tests       # Lint Python
uv run ruff format nba_vault tests      # Format Python
uv run ty check                         # Type check (Astral ty)
uv run sqlfluff lint migrations --dialect sqlite
uv run sqlfluff lint duckdb/views --dialect duckdb
```

### Testing

```bash
# Fast run (no coverage overhead)
uv run pytest

# Parallel run using all CPU cores (~2-3× faster)
uv run pytest -n auto

# With coverage report
uv run pytest --cov=nba_vault --cov-report=term-missing

# Single test file or test
uv run pytest tests/test_schema.py
uv run pytest tests/test_schema.py::test_database_initialization
```

> Coverage is **opt-in** — not included in default `pytest` runs so the test loop stays fast.
> `check.sh` / `check.ps1` always pass `--cov` for CI-quality checks.

### Pre-commit Hooks

```bash
uv run pre-commit install
```

Hooks run automatically on `git commit`:
- Ruff lint (with auto-fix)
- Ruff format
- SQLFluff lint and format for `.sql` files

### Adding a New Ingestor

1. Create `nba_vault/ingestion/my_ingestor.py`
2. Define a Pydantic model in `nba_vault/models/`
3. Implement the three-stage pipeline:

```python
from nba_vault.ingestion import BaseIngestor, register_ingestor
from nba_vault.models import MyModelCreate

@register_ingestor
class MyIngestor(BaseIngestor):
    entity_type = "my_entity"

    def fetch(self, entity_id: str, **kwargs):
        self.rate_limiter.acquire()
        return api_call(entity_id)

    def validate(self, raw: dict):
        return [MyModelCreate(**item) for item in raw["items"]]

    def upsert(self, models, conn):
        conn.execute("BEGIN")
        rows = 0
        for m in models:
            conn.execute(
                "INSERT OR REPLACE INTO my_table VALUES (?, ?)",
                (m.id, m.value),
            )
            rows += conn.execute("SELECT changes()").fetchone()[0]
        conn.execute("COMMIT")
        return rows
```

4. Import the class in `nba_vault/ingestion/__init__.py` (triggers `@register_ingestor`)
5. Add tests in `tests/test_new_ingestors.py`
6. Run `./scripts/fix.sh` to lint and format

### Project Structure

```
nba-vault/
├── nba_vault/
│   ├── cli/                      # CLI interface (typer)
│   │   ├── admin.py              # init, migrate, status, validate
│   │   ├── ingestion.py          # ingest, ingest-players
│   │   ├── advanced_stats.py     # ingest-tracking, ingest-lineups, ingest-team-*
│   │   ├── scrapers.py           # ingest-injuries, ingest-contracts
│   │   └── export.py             # export
│   ├── schema/
│   │   ├── connection.py         # get_db_connection() with optimized PRAGMAs
│   │   └── migrations.py         # yoyo-migrations runner
│   ├── models/                   # Pydantic validation models
│   │   ├── player.py
│   │   ├── team.py
│   │   ├── franchise.py
│   │   ├── game.py
│   │   ├── season.py
│   │   ├── league.py
│   │   ├── coach.py
│   │   ├── official.py
│   │   └── advanced_stats.py
│   ├── ingestion/                # Data ingestion framework
│   │   ├── base.py               # BaseIngestor ABC
│   │   ├── registry.py           # @register_ingestor + discovery helpers
│   │   ├── players.py            # Basketball Reference
│   │   ├── player_tracking.py    # NBA.com tracking metrics
│   │   ├── lineups.py            # NBA.com 5-man lineups
│   │   ├── team_advanced_stats.py
│   │   ├── team_other_stats.py
│   │   ├── injuries.py           # ESPN / Rotowire (web scraping)
│   │   ├── contracts.py          # RealGM / Spotrac (web scraping)
│   │   ├── nba_stats_client.py   # NBA.com API client
│   │   └── basketball_reference.py
│   ├── duckdb/
│   │   └── builder.py            # build_duckdb_database(), create_analytical_views()
│   └── utils/
│       ├── config.py             # Settings (pydantic-settings, lru_cache)
│       ├── logging.py            # structlog setup
│       ├── cache.py              # Filesystem response cache
│       └── rate_limit.py         # Token bucket + retry_with_backoff
├── duckdb/views/                 # 11 analytical view SQL files (DuckDB dialect)
├── migrations/                   # Database migration SQL files (SQLite dialect)
│   ├── 0001_initial_schema.sql
│   ├── 0002_seed_data.sql
│   ├── 0003_missing_features.sql
│   └── 0004_missing_features_indexes.sql
├── scripts/
│   ├── check.sh / check.ps1      # Full quality check + coverage
│   └── fix.sh / fix.ps1          # Auto-fix lint and format
├── tests/
├── pyproject.toml                # Dependencies + ruff, ty, pytest config
├── .sqlfluff                     # SQLFluff configuration
├── .pre-commit-config.yaml       # Pre-commit hooks
├── .python-version               # Pins Python 3.12.8 for uv
├── .env.example                  # Environment variable template
└── PRD.md                        # Product Requirements Document
```

---

## Key Design Decisions

### Franchise vs Team separation

The schema distinguishes between:

- **`franchise`**: The historical entity (e.g., "Lakers" with continuity across relocations)
- **`team`**: A season-specific record that links to a franchise

This allows accurate tracking of team history across city moves and name changes.

### `data_availability_flags` bitmask

Each `team` row carries a bitmask tracking which data types have been loaded (box scores, play-by-play, shot charts, etc.). The bit definitions live in the `data_availability_flag_def` table, making it easy to check coverage or queue missing data types without querying multiple join tables.

### Season format conventions

| Context | Format | Example |
|---|---|---|
| NBA.com API `season` param | `"YYYY-YY"` | `"2023-24"` |
| Internal season ID | `int` (start year) | `2023` |
| Game IDs | 10-character string | `"0022300001"` |
| Team IDs | `int` (NBA.com ID) | `1610612747` (Lakers) |

---

## Tech Stack

| Tool | Role |
|---|---|
| [uv](https://docs.astral.sh/uv/) | Dependency management and virtual environments |
| [Ruff](https://docs.astral.sh/ruff/) | Linter + formatter (replaces flake8, black, isort) |
| [ty](https://docs.astral.sh/ty/) | Type checker by Astral |
| [SQLFluff](https://sqlfluff.com/) | SQL linter and formatter (SQLite + DuckDB dialects) |
| [Pydantic v2](https://docs.pydantic.dev/) | Data validation models |
| [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) | Environment-based configuration |
| [structlog](https://www.structlog.org/) | Structured logging (JSON or console output) |
| [yoyo-migrations](https://ollycope.com/software/yoyo/latest/) | SQL migration runner with rollback support |
| [DuckDB](https://duckdb.org/) | In-process analytical database |
| [nba-api](https://github.com/swar/nba_api) | Python wrapper for stats.nba.com (100+ endpoints) |
| [pytest](https://pytest.org/) + [pytest-xdist](https://pytest-xdist.readthedocs.io/) | Test runner with parallel execution |

---

## Contributing

Contributions are welcome! Please ensure all quality checks pass before opening a PR:

```bash
./scripts/check.sh    # or check.ps1 on Windows
```

See `CONTRIBUTING.md` for detailed guidelines.

---

## License

MIT License — see `LICENSE` for details.

## Data License

Source data is retrieved from stats.nba.com and Basketball-Reference. Users are responsible for compliance with those services' terms of use in their jurisdiction.

## Acknowledgments

- **Astral** — uv, ruff, ty
- [swar/nba_api](https://github.com/swar/nba_api) — Python wrapper for stats.nba.com
- **DuckDB** — Fast in-process analytics
- **Pydantic** — Data validation

---

*This project is in active development. See `PRD.md` for the roadmap and known limitations.*
