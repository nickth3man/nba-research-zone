# Contributing to NBA Vault

Thank you for your interest in contributing to NBA Vault! This document provides guidelines for contributing to the project.

## Development Setup

### Prerequisites

1. **Install uv** (modern Python package manager):
   ```bash
   # macOS/Linux
   curl -LsSf https://astral.sh/uv/install.sh | sh

   # Windows
   powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
   ```

2. **Fork and clone the repository**:
   ```bash
   git clone https://github.com/yourusername/nba-vault.git
   cd nba-vault
   ```

3. **Install dependencies**:
   ```bash
   uv sync --group dev
   ```

## Code Quality Workflow

This project uses modern Rust-based tooling for fast, automated code quality checks:

### Quick Commands

```bash
# Check all code quality (linting, formatting, type checking)
./scripts/check.sh  # macOS/Linux
./scripts/check.ps1  # Windows

# Auto-fix issues
./scripts/fix.sh  # macOS/Linux
./scripts/fix.ps1  # Windows

# Individual tools
uv run ruff check nba_vault tests      # Lint
uv run ruff format nba_vault tests     # Format
uv run ty check                        # Type check
uv run pytest                          # Tests
```

### Pre-commit Hooks

Install pre-commit hooks to automatically check code before commits:

```bash
uv run pre-commit install
```

Now every commit will automatically run ruff for linting and formatting.

### Manual Checks

```bash
# Run all checks
uv run ruff check nba_vault tests
uv run ruff format --check nba_vault tests
uv run ty check
uv run pytest
```

## Code Style

### Ruff Configuration

The project uses **Ruff** as an all-in-one replacement for:
- `black` (formatting)
- `isort` (import sorting)
- `flake8` (linting)
- `pylint` (additional checks)
- `pyupgrade` (syntax modernization)

Configuration is in `pyproject.toml`:
- Line length: 100 characters
- Target Python version: 3.11+
- Comprehensive rule set enabled

### Type Checking with Ty

The project uses **Ty** (by Astral, creators of uv and ruff) for fast type checking:
- Strict mode by default
- Configurable rule severity in `pyproject.toml`
- Excellent error messages and performance

### SQL Code Quality with SQLFluff

The project uses **SQLFluff** for SQL linting and formatting:
- **SQLite dialect** for migration files in `migrations/`
- **DuckDB dialect** for analytical views in `duckdb/views/`
- Auto-fixable formatting issues
- Configurable rules in `.sqlfluff`
- Pre-commit hooks for automatic checking

Configuration highlights:
- Line length: 100 characters (matches Python code)
- Trailing commas enforced
- Flexible capitalization (case-insensitive)
- Dialect-specific optimizations

### Type Hints

- Use type hints for all function parameters and return values
- Import `typing` constructs from `typing` where needed
- Use `|` for union types (Python 3.10+)
- Use `from __future__ import annotations` for forward references

Example:
```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nba_vault.models import Player

def get_player(player_id: int) -> Player | None:
    """Get a player by ID."""
    ...
```

## Testing

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=nba_vault --cov-report=html

# Run specific test file
uv run pytest tests/test_schema.py

# Run specific test
uv run pytest tests/test_schema.py::test_database_initialization
```

### Writing Tests

- Place tests in `tests/` directory
- Name test files: `test_*.py`
- Name test functions: `test_*()`
- Use descriptive names that explain what is being tested
- Follow AAA pattern: Arrange, Act, Assert

Example:
```python
def test_player_model_validation():
    """Test that Player model validates correctly."""
    # Arrange
    player_data = {
        "player_id": 2544,
        "first_name": "LeBron",
        "last_name": "James",
        "full_name": "LeBron James",
        ...
    }

    # Act
    player = PlayerCreate(**player_data)

    # Assert
    assert player.player_id == 2544
    assert player.is_active is True
```

## Commit Messages

Follow conventional commit format:

```
type(scope): description

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `refactor`: Code refactoring
- `test`: Test changes
- `chore`: Maintenance tasks

Example:
```
feat(ingestion): add player biography ingestor

Implement new ingestor for fetching player biographical data
from nba_api CommonPlayerInfo endpoint.

- Add PlayerIngestor class
- Implement fetch(), validate(), upsert() methods
- Add unit tests

Closes #123
```

## Pull Request Process

1. **Create a branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make changes**:
   - Write code following the style guide
   - Add tests for new functionality
   - Run `./scripts/fix.sh` to format your code
   - Run `./scripts/check.sh` to verify everything passes

3. **Commit**:
   ```bash
   git add .
   git commit -m "feat(scope): description"
   ```

4. **Push and create PR**:
   ```bash
   git push origin feature/your-feature-name
   ```
   Then create a pull request on GitHub with a clear description.

## Adding Features

### New Ingestor

1. Create ingestor class in `nba_vault/ingestion/`
2. Inherit from `BaseIngestor`
3. Implement required methods: `fetch()`, `validate()`, `upsert()`
4. Register with `@register_ingestor` decorator
5. Add Pydantic models in `nba_vault/models/`
6. Write tests in `tests/test_ingestion.py`
7. Run `./scripts/fix.sh` to format code

Example:
```python
from nba_vault.ingestion import BaseIngestor, register_ingestor
from nba_vault.models import PlayerCreate

@register_ingestor
class PlayerIngestor(BaseIngestor):
    """Ingest player data from nba_api."""

    entity_type = "player"

    def fetch(self, entity_id: str, **kwargs) -> dict:
        """Fetch player data from nba_api."""
        from nba_api.stats.static import players
        # Implementation
        ...

    def validate(self, raw: dict) -> PlayerCreate:
        """Validate player data with Pydantic."""
        return PlayerCreate(**raw)

    def upsert(self, model: PlayerCreate, conn) -> int:
        """Insert or update player in database."""
        # Implementation
        ...
```

### New DuckDB View

1. Create SQL file in `duckdb/views/v_{view_name}.sql`
2. Follow naming convention: `v_{view_name}.sql`
3. Use `sqlite_db` prefix for SQLite tables
4. Test view manually after building database
5. Run `./scripts/fix.sh` to format your SQL with SQLFluff
6. Verify SQL passes linting: `uv run sqlfluff lint duckdb/views/`

### Database Migration

1. Create migration file: `migrations/{timestamp}_{description}.sql`
2. Use `CREATE TABLE IF NOT EXISTS` for new tables
3. Use `ALTER TABLE` for modifications
4. Test migration on clean database
5. Run `./scripts/fix.sh` to format your SQL with SQLFluff
6. Verify SQL passes linting: `uv run sqlfluff lint migrations/`

## Questions?

Open an issue for discussion before making significant changes.

## Code Review Guidelines

When reviewing code:
- Check for type hints and proper error handling
- Ensure tests cover new functionality
- Verify documentation is updated
- Confirm code style matches project standards
- For SQL files: ensure proper formatting and dialect-specific syntax
- Run `./scripts/check.sh` to verify all quality checks pass
- Test the changes if possible

Thank you for contributing to NBA Vault! 🏀
