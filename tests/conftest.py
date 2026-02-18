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


@pytest.fixture
def db_connection(temp_db_path):
    """Create a database connection for testing."""
    conn = sqlite3.connect(str(temp_db_path))
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
