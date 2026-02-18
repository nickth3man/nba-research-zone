# NBA Vault

A comprehensive, open-source historical NBA database built with SQLite and DuckDB. NBA Vault provides a complete, query-optimized database of NBA/ABA/BAA history from 1946 to present.

## Features

- **Complete Historical Coverage**: All 78+ seasons of NBA/ABA/BAA history
- **Dual-Engine Architecture**: SQLite for durability + DuckDB for analytics
- **Comprehensive Data**: Players, teams, games, box scores, play-by-play, shot charts, awards, draft history, and more
- **Automated Updates**: Incremental daily updates without full re-ingestion
- **Modern Tooling**: Built with uv, ruff, ty, and SQLFluff for blazing-fast development
- **Open Source**: Self-hostable, no external dependencies, no API keys required

## Quick Start

### Prerequisites

- **uv**: Modern Python package manager (10-100x faster than pip)
  ```bash
  # Install uv (macOS/Linux)
  curl -LsSf https://astral.sh/uv/install.sh | sh

  # Install uv (Windows)
  powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
  ```

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/nba-vault.git
cd nba-vault

# Install dependencies with uv (creates virtual environment automatically)
uv sync

# Or install development dependencies
uv sync --group dev
```

### Development Workflow

This project uses modern Rust-based Python tooling for maximum speed:

```bash
# Run all checks (linting, formatting, type checking)
# Linux/macOS:
./scripts/check.sh
# Windows:
./scripts/check.ps1

# Auto-fix linting and formatting issues
# Linux/macOS:
./scripts/fix.sh
# Windows:
./scripts/fix.ps1

# Run tests
uv run pytest

# Type check with ty
uv run ty check

# Lint with ruff
uv run ruff check nba_vault tests

# Format with ruff
uv run ruff format nba_vault tests

# Lint SQL with SQLFluff
uv run sqlfluff lint migrations --dialect sqlite
uv run sqlfluff lint duckdb/views --dialect duckdb

# Format SQL with SQLFluff
uv run sqlfluff fix migrations --dialect sqlite --force
uv run sqlfluff fix duckdb/views --dialect duckdb --force

# Run the CLI
uv run nba-vault --help
```

### Configuration

```bash
# Copy environment variables
cp .env.example .env

# Initialize the database
uv run nba-vault init
```

### Usage

```bash
# Run a full historical backfill (takes 12-72 hours depending on settings)
uv run nba-vault ingest --mode full --workers 4

# Run daily incremental update
uv run nba-vault ingest --mode incremental

# Export to DuckDB for analytics
uv run nba-vault export --format duckdb

# Check database status
uv run nba-vault status

# Validate data integrity
uv run nba-vault validate
```

## Architecture

### Modern Python Tooling Stack

- **uv**: Blazing-fast package manager (replaces pip, pip-tools, virtualenv)
- **Ruff**: All-in-one linter and formatter (replaces black, isort, flake8, pylint, etc.)
- **Ty**: Fast type checker by Astral (replaces mypy/pyright)
- **SQLFluff**: SQL linter and formatter (supports SQLite and DuckDB dialects)

This combination provides:
- 10-100x faster dependency resolution and installation
- Instantaneous linting and formatting
- Modern type checking with excellent error messages
- SQL code quality enforcement with dialect-specific rules
- Unified configuration in `pyproject.toml` and `.sqlfluff`

### Data Storage

NBA Vault uses a dual-engine architecture:

- **SQLite**: Source of truth, optimized for transactional queries and data integrity
- **DuckDB**: Analytical engine with pre-built views for complex aggregations

### Data Sources

- **Primary**: `swar/nba_api` - stats.nba.com endpoints (100+ endpoints)
- **Fallback**: Basketball-Reference (with rate limiting)
- **Static**: ABA era data, historical awards, seed data

### Schema

The database includes 30+ tables covering:
- Leagues, seasons, franchises, teams
- Players, coaches, officials
- Games (regular season, playoffs, preseason)
- Box scores (traditional, advanced, hustle)
- Play-by-play events
- Shot charts
- Draft history
- Awards
- Transactions

See `PRD.md` for complete schema documentation.

## Configuration

Edit `.env` to customize settings:

```bash
# Database paths
DB_PATH=nba.sqlite
DUCKDB_PATH=nba.duckdb

# API rate limiting
NBA_API_RATE_LIMIT=8  # requests per minute
BACKFILL_WORKERS=1    # parallel workers

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json
```

## Development

### Code Quality Tools

This project uses automated code quality enforcement:

```bash
# Install pre-commit hooks (runs before commits)
uv sync --group dev
uv run pre-commit install

# Manual checks
./scripts/check.sh  # or check.ps1 on Windows

# Auto-fix issues
./scripts/fix.sh    # or fix.ps1 on Windows
```

### Ruff Configuration

The `pyproject.toml` includes comprehensive ruff configuration:
- Linting: Replaces flake8, pylint, isort, pyupgrade, etc.
- Formatting: Replaces black
- Import sorting: Automatic and deterministic

### Ty Configuration

Type checking is configured in `pyproject.toml`:
- Strict mode by default for well-typed code
- Configurable rule severity (error/warn/ignore)
- Fast type checking with excellent error messages

### SQLFluff Configuration

SQL code quality is enforced with SQLFluff (`.sqlfluff`):
- **SQLite dialect** for migrations
- **DuckDB dialect** for analytical views
- Auto-fixable linting issues
- Configurable rules for SQL style and best practices
- Pre-commit hooks for automatic checking

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=nba_vault --cov-report=html

# Run specific test file
uv run pytest tests/test_schema.py
```

### Project Structure

```
nba-vault/
├── nba_vault/              # Main package
│   ├── cli.py              # CLI interface
│   ├── schema/             # Database schema and migrations
│   ├── models/             # Pydantic validation models
│   ├── ingestion/          # Data ingestion framework
│   ├── duckdb/             # DuckDB views and builder
│   └── utils/              # Configuration, logging, caching
├── duckdb/views/           # Analytical view definitions
├── migrations/             # Database migration files
├── scripts/                # Development workflow scripts
├── tests/                  # Test suite
├── pyproject.toml          # Project configuration (uv, ruff, ty)
├── .env.example            # Environment variables template
├── pre-commit-config.yaml  # Pre-commit hooks (ruff)
└── PRD.md                  # Product Requirements Document
```

## Contributing

Contributions are welcome! Please see `CONTRIBUTING.md` for guidelines.

### Adding a New Ingestor

1. Create a class inheriting from `BaseIngestor`
2. Implement `fetch()`, `validate()`, and `upsert()` methods
3. Register with `@register_ingestor` decorator
4. Add unit tests
5. Run `./scripts/fix.sh` to format your code

Example:

```python
from nba_vault.ingestion import BaseIngestor, register_ingestor
from nba_vault.models import PlayerCreate

@register_ingestor
class PlayerIngestor(BaseIngestor):
    entity_type = "player"

    def fetch(self, entity_id: str, **kwargs):
        # Fetch from nba_api
        ...

    def validate(self, raw: dict):
        # Validate with Pydantic
        return PlayerCreate(**raw)

    def upsert(self, model: PlayerCreate, conn):
        # Insert to database
        ...
```

## License

MIT License - See LICENSE file for details.

## Data License

Source data from stats.nba.com and Basketball-Reference. Users are responsible for compliance with terms of service in their jurisdiction.

## Acknowledgments

- **uv & ruff & ty** by Astral - Modern Python toolchain
- `swar/nba_api` - Python wrapper for stats.nba.com endpoints
- DuckDB - Analytical database engine
- Pydantic - Data validation library

---

**Note**: This project is in active development. See PRD.md for roadmap and known limitations.
