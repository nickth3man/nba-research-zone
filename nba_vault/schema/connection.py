"""Database connection management."""

import sqlite3
from pathlib import Path
from typing import Optional

import structlog

from nba_vault.utils.config import get_settings

logger = structlog.get_logger(__name__)


def get_db_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """
    Get a SQLite database connection with optimized settings.

    Args:
        db_path: Path to the database file. If None, uses default from settings.

    Returns:
        SQLite connection with PRAGMAs configured for performance and data integrity.
    """
    settings = get_settings()
    db_path = db_path or Path(settings.db_path)

    # Ensure parent directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Apply performance optimizations
    conn.execute("PRAGMA page_size = 16384")  # 16 KB pages
    conn.execute("PRAGMA journal_mode = WAL")  # Write-ahead logging
    conn.execute("PRAGMA synchronous = NORMAL")  # Adequate durability with WAL
    conn.execute("PRAGMA foreign_keys = ON")  # Enforce FK integrity
    conn.execute("PRAGMA cache_size = -131072")  # 128 MB cache
    conn.execute("PRAGMA temp_store = MEMORY")  # Use memory for temp tables

    logger.debug("Database connection established", db_path=str(db_path))
    return conn


def init_database(db_path: Optional[Path] = None) -> None:
    """
    Initialize the database schema.

    This should be called once on first run or after schema changes.

    Args:
        db_path: Path to the database file. If None, uses default from settings.
    """
    from nba_vault.schema.migrations import run_migrations

    conn = get_db_connection(db_path)
    try:
        run_migrations(conn)
        logger.info("Database initialized successfully")
    finally:
        conn.close()


def close_connection(conn: sqlite3.Connection) -> None:
    """
    Close a database connection cleanly.

    Args:
        conn: The SQLite connection to close.
    """
    conn.close()
    logger.debug("Database connection closed")
