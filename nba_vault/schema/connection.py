"""Database connection management."""

import sqlite3
from pathlib import Path

import structlog

from nba_vault.utils.config import get_settings

logger = structlog.get_logger(__name__)


def get_db_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """
    Get a SQLite database connection with optimized settings.

    Args:
        db_path: Path to the database file. If None, uses default from settings.

    Returns:
        SQLite connection with PRAGMAs configured for performance and data integrity.

    Raises:
        RuntimeError: If the database directory cannot be created or the database
            cannot be opened.
    """
    settings = get_settings()
    db_path = db_path or Path(settings.db_path)

    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
    except (PermissionError, OSError) as e:
        raise RuntimeError(f"Cannot create database directory '{db_path.parent}': {e}") from e

    try:
        # isolation_level=None disables Python's implicit transaction management.
        # All ingestors manage transactions explicitly via BEGIN/COMMIT/ROLLBACK,
        # so autocommit mode is correct and avoids "cannot start a transaction
        # within a transaction" errors when ingestors call conn.execute("BEGIN")
        # after upsert_audit() has implicitly opened a transaction.
        conn = sqlite3.connect(str(db_path), isolation_level=None)
    except sqlite3.OperationalError as e:
        raise RuntimeError(f"Cannot open database at '{db_path}': {e}") from e

    conn.row_factory = sqlite3.Row

    try:
        conn.execute("PRAGMA page_size = 16384")  # 16 KB pages
        conn.execute("PRAGMA journal_mode = WAL")  # Write-ahead logging
        conn.execute("PRAGMA synchronous = NORMAL")  # Adequate durability with WAL
        conn.execute("PRAGMA foreign_keys = ON")  # Enforce FK integrity
        conn.execute("PRAGMA cache_size = -131072")  # 128 MB cache
        conn.execute("PRAGMA temp_store = MEMORY")  # Use memory for temp tables
    except sqlite3.Error as e:
        conn.close()
        raise RuntimeError(f"Failed to configure database pragmas for '{db_path}': {e}") from e

    logger.debug("Database connection established", db_path=str(db_path))
    return conn


def init_database(db_path: Path | None = None) -> None:
    """
    Initialize the database schema.

    This should be called once on first run or after schema changes.

    Args:
        db_path: Path to the database file. If None, uses default from settings.

    Raises:
        RuntimeError: If the database cannot be opened or migrations fail.
    """
    from nba_vault.schema.migrations import run_migrations  # noqa: PLC0415

    conn = get_db_connection(db_path)
    conn.close()
    try:
        run_migrations(db_path)
        logger.info("Database initialized successfully")
    except Exception as e:
        raise RuntimeError(f"Failed to initialize database: {e}") from e


def close_connection(conn: sqlite3.Connection) -> None:
    """
    Close a database connection cleanly.

    Args:
        conn: The SQLite connection to close.
    """
    conn.close()
    logger.debug("Database connection closed")
