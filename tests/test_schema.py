"""Tests for database schema and migrations."""

import sqlite3
import pytest
from pathlib import Path


def test_database_initialization(temp_db_path):
    """Test that database initializes correctly."""
    from nba_vault.schema.connection import init_database

    init_database(temp_db_path)

    # Verify database was created
    assert temp_db_path.exists()

    # Verify tables exist
    conn = sqlite3.connect(temp_db_path)
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]

    # Check core tables exist
    assert "league" in tables
    assert "season" in tables
    assert "franchise" in tables
    assert "team" in tables
    assert "player" in tables
    assert "game" in tables
    assert "player_game_log" in tables
    assert "ingestion_audit" in tables

    conn.close()


def test_seed_data(temp_db_path):
    """Test that seed data is inserted correctly."""
    from nba_vault.schema.connection import init_database

    init_database(temp_db_path)

    conn = sqlite3.connect(temp_db_path)

    # Check leagues
    cursor = conn.execute("SELECT COUNT(*) FROM league")
    league_count = cursor.fetchone()[0]
    assert league_count >= 3  # BAA, NBA, ABA

    # Check data availability flags
    cursor = conn.execute("SELECT COUNT(*) FROM data_availability_flag_def")
    flag_count = cursor.fetchone()[0]
    assert flag_count >= 7  # At least 7 flag definitions

    # Check event message types
    cursor = conn.execute("SELECT COUNT(*) FROM event_message_type")
    event_count = cursor.fetchone()[0]
    assert event_count >= 21  # All NBA event types

    conn.close()


def test_foreign_key_constraints(temp_db_path):
    """Test that foreign key constraints are enforced."""
    from nba_vault.schema.connection import init_database

    init_database(temp_db_path)

    conn = sqlite3.connect(temp_db_path)
    conn.execute("PRAGMA foreign_keys = ON")

    # Try to insert a game with invalid season_id
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """
            INSERT INTO game (game_id, season_id, game_date, game_type,
                           home_team_id, away_team_id)
            VALUES ('TEST001', 99999, '2024-01-01', 'Regular Season', 1, 2)
            """
        )

    conn.close()


def test_indexes_created(temp_db_path):
    """Test that performance indexes are created."""
    from nba_vault.schema.connection import init_database

    init_database(temp_db_path)

    conn = sqlite3.connect(temp_db_path)

    # Check that indexes exist
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
    )
    indexes = [row[0] for row in cursor.fetchall()]

    # Check some key indexes exist
    assert "idx_player_game_log_season_player" in indexes
    assert "idx_game_season_type" in indexes
    assert "idx_ingestion_audit_entity" in indexes

    conn.close()
