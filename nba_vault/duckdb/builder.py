"""DuckDB database builder and view manager."""

from pathlib import Path

import duckdb
import structlog

from nba_vault.utils.config import get_settings

logger = structlog.get_logger(__name__)


def build_duckdb_database(sqlite_path: Path | str | None = None, duckdb_path: Path | str | None = None) -> None:
    """
    Build the DuckDB analytical database from SQLite data.

    This function creates a DuckDB database, attaches the SQLite database,
    and creates analytical views for optimized queries.

    Args:
        sqlite_path: Path to SQLite database. If None, uses settings.
        duckdb_path: Path to DuckDB database. If None, uses settings.
    """
    settings = get_settings()
    sqlite_path = sqlite_path or settings.db_path
    duckdb_path = duckdb_path or settings.duckdb_path

    sqlite_path = Path(sqlite_path)
    duckdb_path = Path(duckdb_path)

    if not sqlite_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {sqlite_path}")

    logger.info("Building DuckDB database", sqlite=str(sqlite_path), duckdb=str(duckdb_path))

    # Create DuckDB connection
    con = duckdb.connect(str(duckdb_path))

    try:
        # Configure DuckDB
        con.execute(f"SET memory_limit = '{settings.duckdb_memory_limit}'")
        con.execute(f"SET threads = {settings.duckdb_threads}")

        # Install and load sqlite extension
        con.execute("INSTALL sqlite")
        con.execute("LOAD sqlite")

        # Attach SQLite database
        con.execute(f"ATTACH '{sqlite_path}' AS sqlite_db (READ_ONLY)")

        logger.info("SQLite database attached")

        # Create analytical views
        create_analytical_views(con)

        logger.info("DuckDB database built successfully")

    except Exception as e:
        logger.error("Failed to build DuckDB database", error=str(e))
        raise
    finally:
        con.close()


def refresh_views(sqlite_path: Path | str | None = None, duckdb_path: Path | str | None = None) -> None:
    """
    Refresh DuckDB materialized views after SQLite updates.

    Args:
        sqlite_path: Path to SQLite database. If None, uses settings.
        duckdb_path: Path to DuckDB database. If None, uses settings.
    """
    settings = get_settings()
    sqlite_path = sqlite_path or settings.db_path
    duckdb_path = duckdb_path or settings.duckdb_path

    duckdb_path = Path(duckdb_path)

    if not duckdb_path.exists():
        logger.warning("DuckDB database not found, building new database")
        build_duckdb_database(sqlite_path, duckdb_path)
        return

    logger.info("Refreshing DuckDB views")

    con = duckdb.connect(str(duckdb_path))

    try:
        # Ensure sqlite extension is loaded
        con.execute("LOAD sqlite")
        con.execute(f"ATTACH '{sqlite_path}' AS sqlite_db (READ_ONLY)")

        # Recreate views
        create_analytical_views(con)

        logger.info("DuckDB views refreshed")

    except Exception as e:
        logger.error("Failed to refresh DuckDB views", error=str(e))
        raise
    finally:
        con.close()


def create_analytical_views(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create analytical views in DuckDB.

    Args:
        con: DuckDB connection.
    """
    # Load view definitions from SQL files
    views_dir = Path(__file__).parent.parent.parent / "duckdb" / "views"

    if not views_dir.exists():
        logger.warning("Views directory not found, skipping view creation")
        return

    view_files = sorted(views_dir.glob("*.sql"))

    for view_file in view_files:
        view_name = view_file.stem.replace("v_", "")
        logger.info(f"Creating view: {view_name}")

        try:
            sql = view_file.read_text()
            con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {sql}")
            logger.info(f"View created: {view_name}")
        except Exception as e:
            logger.error(f"Failed to create view {view_name}", error=str(e))
            raise
