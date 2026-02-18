"""Tests for 0%-covered infrastructure modules.

Covers:
- nba_vault/schema/connection.py
- nba_vault/schema/migrations.py
- nba_vault/ingestion/audit.py
- nba_vault/duckdb/builder.py
"""

import sqlite3
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# connection.py
# ---------------------------------------------------------------------------


def test_get_db_connection_returns_valid_connection(temp_db_path):
    """get_db_connection() should return a working sqlite3.Connection."""
    from nba_vault.schema.connection import get_db_connection

    conn = get_db_connection(temp_db_path)
    try:
        assert isinstance(conn, sqlite3.Connection)
        # A simple query proves the connection is alive
        result = conn.execute("SELECT 1").fetchone()
        assert result[0] == 1
    finally:
        conn.close()


def test_get_db_connection_wal_mode(temp_db_path):
    """get_db_connection() should enable WAL journal mode."""
    from nba_vault.schema.connection import get_db_connection

    conn = get_db_connection(temp_db_path)
    try:
        row = conn.execute("PRAGMA journal_mode").fetchone()
        assert row[0].lower() == "wal"
    finally:
        conn.close()


def test_get_db_connection_foreign_keys_enabled(temp_db_path):
    """get_db_connection() should turn on foreign-key enforcement."""
    from nba_vault.schema.connection import get_db_connection

    conn = get_db_connection(temp_db_path)
    try:
        row = conn.execute("PRAGMA foreign_keys").fetchone()
        assert row[0] == 1
    finally:
        conn.close()


def test_get_db_connection_row_factory(temp_db_path):
    """get_db_connection() should set row_factory to sqlite3.Row."""
    from nba_vault.schema.connection import get_db_connection

    conn = get_db_connection(temp_db_path)
    try:
        assert conn.row_factory is sqlite3.Row
    finally:
        conn.close()


def test_init_database_runs_without_error(temp_db_path):
    """init_database() should run migrations and create the expected tables."""
    from nba_vault.schema.connection import init_database

    init_database(temp_db_path)

    assert temp_db_path.exists()

    conn = sqlite3.connect(str(temp_db_path))
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cursor.fetchall()}
    conn.close()

    assert "player" in tables
    assert "game" in tables
    assert "ingestion_audit" in tables


def test_close_connection_closes(temp_db_path):
    """close_connection() should prevent further use of the connection."""
    from nba_vault.schema.connection import close_connection, get_db_connection

    conn = get_db_connection(temp_db_path)
    close_connection(conn)

    with pytest.raises(sqlite3.ProgrammingError):
        conn.execute("SELECT 1")


# ---------------------------------------------------------------------------
# migrations.py
# ---------------------------------------------------------------------------


def test_get_migrations_dir_returns_existing_path():
    """get_migrations_dir() should return a directory that actually exists."""
    from nba_vault.schema.migrations import get_migrations_dir

    migrations_dir = get_migrations_dir()
    assert isinstance(migrations_dir, Path)
    assert migrations_dir.exists()
    assert migrations_dir.is_dir()


def test_run_migrations_applies_schema(temp_db_path):
    """run_migrations() should create the core schema tables."""
    from nba_vault.schema.migrations import run_migrations

    run_migrations(temp_db_path)

    conn = sqlite3.connect(str(temp_db_path))
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cursor.fetchall()}
    conn.close()

    assert "player" in tables
    assert "game" in tables


def test_run_migrations_is_idempotent(temp_db_path):
    """run_migrations() called twice should not raise any error."""
    from nba_vault.schema.migrations import run_migrations

    run_migrations(temp_db_path)
    # Second call should be a no-op (all migrations already applied)
    run_migrations(temp_db_path)


def test_rollback_migration_steps_zero(temp_db_path):
    """rollback_migration(steps=0) on an empty database should not raise."""
    from nba_vault.schema.migrations import rollback_migration

    # No migrations applied yet — rolling back 0 steps is always a no-op
    rollback_migration(temp_db_path, steps=0)


def test_rollback_migration_steps_one(temp_db_path):
    """rollback_migration(steps=1) after applying migrations should not raise."""
    from nba_vault.schema.migrations import rollback_migration, run_migrations

    run_migrations(temp_db_path)
    # Roll back the last migration — yoyo removes it from the applied table
    rollback_migration(temp_db_path, steps=1)


# ---------------------------------------------------------------------------
# audit.py
# ---------------------------------------------------------------------------


def test_audit_log_success_inserts_row(db_connection):
    """AuditLogger.log() with status=SUCCESS should write a row to ingestion_audit."""
    from nba_vault.ingestion.audit import AuditLogger

    audit = AuditLogger(db_connection)
    audit.log(
        entity_type="player",
        entity_id="jamesle01",
        source="basketball_reference",
        status="SUCCESS",
        row_count=1,
    )

    row = db_connection.execute(
        "SELECT status, row_count FROM ingestion_audit "
        "WHERE entity_type='player' AND entity_id='jamesle01'"
    ).fetchone()

    assert row is not None
    assert row["status"] == "SUCCESS"
    assert row["row_count"] == 1


def test_audit_log_failed_inserts_row(db_connection):
    """AuditLogger.log() with status=FAILED should store the error message."""
    from nba_vault.ingestion.audit import AuditLogger

    audit = AuditLogger(db_connection)
    audit.log(
        entity_type="player",
        entity_id="does_not_exist",
        source="basketball_reference",
        status="FAILED",
        row_count=0,
        error_message="HTTP 404",
    )

    row = db_connection.execute(
        "SELECT status, error_message FROM ingestion_audit "
        "WHERE entity_type='player' AND entity_id='does_not_exist'"
    ).fetchone()

    assert row is not None
    assert row["status"] == "FAILED"
    assert row["error_message"] == "HTTP 404"


def test_audit_timestamps_are_utc(db_connection):
    """AuditLogger.log() should store a UTC ISO timestamp."""
    from nba_vault.ingestion.audit import AuditLogger

    audit = AuditLogger(db_connection)
    audit.log(
        entity_type="game",
        entity_id="0022300001",
        source="nba_api",
        status="SUCCESS",
        row_count=5,
    )

    row = db_connection.execute(
        "SELECT ingest_ts FROM ingestion_audit WHERE entity_type='game' AND entity_id='0022300001'"
    ).fetchone()

    assert row is not None
    ts_str: str = row["ingest_ts"]
    # Must be a non-empty ISO-formatted string that includes UTC offset
    assert ts_str, "ingest_ts should not be empty"
    assert "+00:00" in ts_str, f"Expected UTC-aware timestamp ('+00:00'), got: {ts_str!r}"


def test_audit_get_status(db_connection):
    """AuditLogger.get_status() should return the most-recently-logged entry."""
    from nba_vault.ingestion.audit import AuditLogger

    audit = AuditLogger(db_connection)
    audit.log(
        entity_type="team",
        entity_id="1610612747",
        source="nba_api",
        status="SUCCESS",
        row_count=3,
    )

    status = audit.get_status("team", "1610612747")
    assert status is not None
    assert status["entity_type"] == "team"
    assert status["entity_id"] == "1610612747"
    assert status["status"] == "SUCCESS"


def test_audit_get_status_missing_returns_none(db_connection):
    """AuditLogger.get_status() should return None for an unknown entity."""
    from nba_vault.ingestion.audit import AuditLogger

    audit = AuditLogger(db_connection)
    assert audit.get_status("player", "no_such_id") is None


def test_audit_get_failed_entities(db_connection):
    """AuditLogger.get_failed_entities() should list only FAILED rows."""
    from nba_vault.ingestion.audit import AuditLogger

    audit = AuditLogger(db_connection)
    audit.log("player", "bad_player_1", "nba_api", "FAILED", error_message="err1")
    audit.log("player", "bad_player_2", "nba_api", "FAILED", error_message="err2")
    audit.log("player", "good_player", "nba_api", "SUCCESS", row_count=1)

    failed = audit.get_failed_entities(entity_type="player")
    entity_ids = {f["entity_id"] for f in failed}

    assert "bad_player_1" in entity_ids
    assert "bad_player_2" in entity_ids
    assert "good_player" not in entity_ids


# ---------------------------------------------------------------------------
# duckdb/builder.py
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_db_path(tmp_path):
    """A fully migrated SQLite database in a temp directory."""
    from nba_vault.schema.migrations import run_migrations

    db = tmp_path / "test_nba.sqlite"
    run_migrations(db)
    return db


def test_build_duckdb_database_creates_file(sqlite_db_path, tmp_path):
    """build_duckdb_database() should produce a .duckdb file on disk."""
    from nba_vault.duckdb.builder import build_duckdb_database

    duckdb_out = tmp_path / "test_nba.duckdb"
    build_duckdb_database(sqlite_path=sqlite_db_path, duckdb_path=duckdb_out)

    assert duckdb_out.exists()
    assert duckdb_out.stat().st_size > 0


def test_build_duckdb_database_missing_sqlite_raises(tmp_path):
    """build_duckdb_database() should raise FileNotFoundError when SQLite DB is absent."""
    from nba_vault.duckdb.builder import build_duckdb_database

    with pytest.raises(FileNotFoundError):
        build_duckdb_database(
            sqlite_path=tmp_path / "nonexistent.sqlite",
            duckdb_path=tmp_path / "out.duckdb",
        )


def test_refresh_views_builds_when_duckdb_missing(sqlite_db_path, tmp_path):
    """refresh_views() should build a new database when the .duckdb file doesn't exist."""
    from nba_vault.duckdb.builder import refresh_views

    duckdb_out = tmp_path / "test_refresh.duckdb"
    assert not duckdb_out.exists()

    refresh_views(sqlite_path=sqlite_db_path, duckdb_path=duckdb_out)

    assert duckdb_out.exists()


def test_refresh_views_runs_on_existing_database(sqlite_db_path, tmp_path):
    """refresh_views() should run without error on an already-built database."""
    from nba_vault.duckdb.builder import build_duckdb_database, refresh_views

    duckdb_out = tmp_path / "test_existing.duckdb"
    build_duckdb_database(sqlite_path=sqlite_db_path, duckdb_path=duckdb_out)

    # Should not raise
    refresh_views(sqlite_path=sqlite_db_path, duckdb_path=duckdb_out)
