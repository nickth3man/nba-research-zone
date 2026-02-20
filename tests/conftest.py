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


@pytest.fixture
def mock_conn():
    """Return a mock SQLite connection with sensible cursor defaults."""
    from unittest.mock import MagicMock

    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = (0,)
    conn.execute.return_value = cursor
    return conn


@pytest.fixture
def mock_ingestor():
    """Return a mock ingestor."""
    from unittest.mock import MagicMock

    def _create_mock(status="SUCCESS", rows=5, error_message=None):
        ingestor = MagicMock()
        result = {"status": status, "rows_affected": rows}
        if error_message:
            result["error_message"] = error_message
        ingestor.ingest.return_value = result
        return ingestor

    return _create_mock


@pytest.fixture
def patch_db_connection(mock_conn):
    """Patch get_db_connection globally for CLI tests."""
    from unittest.mock import patch

    with (
        patch("nba_vault.cli.admin.get_db_connection", return_value=mock_conn),
        patch("nba_vault.cli.ingestion.get_db_connection", return_value=mock_conn),
        patch("nba_vault.cli.advanced_stats.get_db_connection", return_value=mock_conn),
        patch("nba_vault.cli.scrapers.get_db_connection", return_value=mock_conn),
        patch("nba_vault.cli.export.get_db_connection", return_value=mock_conn),
    ):
        yield mock_conn


@pytest.fixture
def patch_create_ingestor(mock_ingestor):
    """Patch create_ingestor globally for CLI tests."""
    from unittest.mock import patch

    # We will return the default mock ingestor (SUCCESS) by default
    default_ingestor = mock_ingestor()
    with patch("nba_vault.ingestion.create_ingestor", return_value=default_ingestor) as mock:
        yield mock


@pytest.fixture
def patch_settings(sample_settings):
    """Patch get_settings globally for CLI tests."""
    from unittest.mock import patch

    with (
        patch("nba_vault.cli.admin.get_settings", return_value=sample_settings),
        patch("nba_vault.utils.config.get_settings", return_value=sample_settings),
    ):
        yield sample_settings
