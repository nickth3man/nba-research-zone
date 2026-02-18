"""Pytest configuration and fixtures."""

import sqlite3
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_db_path():
    """Create a temporary database path for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        yield Path(f.name)
    # Cleanup is automatic


@pytest.fixture(scope="session")
def migrated_db_path(tmp_path_factory):
    """Run migrations exactly once per session into a shared temp database.

    Using session scope means the ~0.17s yoyo migration cost is paid once
    regardless of how many tests use db_connection, instead of once per test.
    tmp_path_factory is session-scoped and xdist-safe — each worker gets its
    own unique temp directory automatically.
    """
    from nba_vault.schema.migrations import run_migrations

    db = tmp_path_factory.mktemp("session_db") / "test.db"
    run_migrations(db)
    return db


@pytest.fixture
def db_connection(migrated_db_path):
    """Create a database connection for testing against the session-migrated DB.

    Opens a fresh sqlite3 connection per test (no transaction wrapping) because
    ingestors manage their own BEGIN/COMMIT internally — a fixture-level BEGIN
    would cause 'cannot start a transaction within a transaction' in SQLite.
    Tests use distinct entity IDs so accumulated data across tests is safe.
    """
    conn = sqlite3.connect(str(migrated_db_path))
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@pytest.fixture
def sample_settings():
    """Sample settings for testing."""
    from nba_vault.utils.config import Settings

    return Settings(
        db_path=":memory:",
        cache_enabled=False,
        log_level="DEBUG",
        log_format="console",
    )
