"""Shared validation utilities for all ingestors.

This module provides cross-cutting validation concerns:
- Era/season availability gates (prevent API calls for data that doesn't exist)
- FK pre-validation (verify referenced rows exist before upsert)
- data_availability_flags bitmask helpers
- Row-level quarantine for schema-drift records

These utilities are called from individual ingestors and from BaseIngestor.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Era / Season Availability Gates
# ---------------------------------------------------------------------------
# Each entry maps an entity type string to a callable that takes a
# season start year (integer, e.g. 2023 for the 2023-24 season) and
# returns True if data is expected to be available for that year.
#
# Callers should call check_data_availability() rather than this dict directly.

AVAILABILITY_GATES: dict[str, Any] = {
    # Traditional box scores exist for essentially all seasons on NBA.com
    "boxscore_traditional": lambda y: y >= 1946,
    # Advanced box scores only become meaningful/available in digital era
    "boxscore_advanced": lambda y: y >= 1996,
    # Hustle stats launched with the 2015-16 season
    "hustle_stats": lambda y: y >= 2015,
    # Play-by-play available from approximately 1996-97 onward
    "play_by_play": lambda y: y >= 1996,
    # Shot chart coordinate data available from approximately 1996-97 onward
    "shot_chart": lambda y: y >= 1996,
    # Official/referee assignments available from approximately 1990-91 onward
    "officials": lambda y: y >= 1990,
    # Player tracking (speed/distance) launched with the 2013-14 season
    "tracking_stats": lambda y: y >= 2013,
    # Draft combine measurements available from approximately 2000 onward
    "draft_combine": lambda y: y >= 2000,
    # Three-point line introduced in 1979-80
    "three_point_stats": lambda y: y >= 1979,
    # Turnover data standardised from 1977-78
    "turnover_stats": lambda y: y >= 1977,
    # Player awards data available across full history
    "awards": lambda y: y >= 1946,
    # Draft history available from the first BAA draft (1947 season)
    "draft": lambda y: y >= 1947,
    # Coaching staff data available from full history
    "coaches": lambda y: y >= 1946,
}


def check_data_availability(entity_type: str, season_year: int) -> bool:
    """
    Return True if data is expected to be available for the given season.

    Args:
        entity_type: One of the keys in AVAILABILITY_GATES.
        season_year: Integer start year of the season (e.g. 2023 for 2023-24).

    Returns:
        True if data should be available; False if it predates coverage.
        Returns True for unknown entity types (permissive default).
    """
    gate = AVAILABILITY_GATES.get(entity_type)
    if gate is None:
        # Unknown entity type — allow by default, warn
        logger.debug(
            "No availability gate defined for entity type; allowing",
            entity_type=entity_type,
            season_year=season_year,
        )
        return True
    available = gate(season_year)
    if not available:
        logger.debug(
            "Data not available for era",
            entity_type=entity_type,
            season_year=season_year,
        )
    return available


# ---------------------------------------------------------------------------
# FK Pre-Validation
# ---------------------------------------------------------------------------


def require_fk(
    conn: sqlite3.Connection,
    table: str,
    col: str,
    val: Any,
) -> bool:
    """
    Verify that a FK target row exists before attempting a dependent INSERT.

    Args:
        conn: Open SQLite connection (FK enforcement must be ON).
        table: Name of the referenced table (e.g. "player").
        col: Name of the referenced column (e.g. "player_id").
        val: Value that must exist in table.col.

    Returns:
        True if the row exists, False otherwise.

    Example:
        if not require_fk(conn, "player", "player_id", player_id):
            quarantine_row(entity_type, entity_id, row, "FK player.player_id missing")
            continue
    """
    try:
        cur = conn.execute(
            f"SELECT 1 FROM {table} WHERE {col} = ? LIMIT 1",  # noqa: S608
            (val,),
        )
        return cur.fetchone() is not None
    except sqlite3.Error as e:
        logger.warning(
            "FK check query failed",
            table=table,
            col=col,
            val=val,
            error=str(e),
        )
        return False


# ---------------------------------------------------------------------------
# data_availability_flags helpers
# ---------------------------------------------------------------------------

# Cache of flag_name -> bit_value loaded from DB
_flag_cache: dict[str, int] = {}


def get_flag_bit(conn: sqlite3.Connection, flag_name: str) -> int | None:
    """
    Look up the integer bit value for a named data availability flag.

    Args:
        conn: Open SQLite connection.
        flag_name: e.g. "BOXSCORE_TRADITIONAL", "PLAY_BY_PLAY".

    Returns:
        Bit value (power of 2) or None if the flag name is unknown.
    """
    if flag_name in _flag_cache:
        return _flag_cache[flag_name]
    try:
        cur = conn.execute(
            "SELECT bit_value FROM data_availability_flag_def WHERE flag_name = ?",
            (flag_name,),
        )
        row = cur.fetchone()
        if row:
            _flag_cache[flag_name] = row[0]
            return row[0]
    except sqlite3.Error as e:
        logger.warning("Failed to look up flag bit", flag_name=flag_name, error=str(e))
    return None


def set_game_availability_flag(
    conn: sqlite3.Connection,
    game_id: str,
    flag_name: str,
) -> bool:
    """
    Set a data_availability_flags bit on the game row identified by game_id.

    Should be called after a successful upsert of per-game data (box scores,
    play-by-play, shot charts, officials).

    Args:
        conn: Open SQLite connection.
        game_id: 10-character NBA.com game ID.
        flag_name: e.g. "BOXSCORE_TRADITIONAL", "PLAY_BY_PLAY".

    Returns:
        True if the flag was set; False if the game row or flag is unknown.
    """
    bit = get_flag_bit(conn, flag_name)
    if bit is None:
        logger.warning("Unknown availability flag; skipping", flag_name=flag_name)
        return False
    try:
        conn.execute(
            "UPDATE game SET data_availability_flags = data_availability_flags | ? "
            "WHERE game_id = ?",
            (bit, game_id),
        )
        return True
    except sqlite3.Error as e:
        logger.warning(
            "Failed to set availability flag on game",
            game_id=game_id,
            flag_name=flag_name,
            error=str(e),
        )
        return False


def set_player_availability_flag(
    conn: sqlite3.Connection,
    player_id: int,
    flag_name: str,
) -> bool:
    """
    Set a data_availability_flags bit on the player row identified by player_id.

    Args:
        conn: Open SQLite connection.
        player_id: NBA.com player ID.
        flag_name: e.g. "TRACKING_STATS".

    Returns:
        True if the flag was set; False if the player row or flag is unknown.
    """
    bit = get_flag_bit(conn, flag_name)
    if bit is None:
        logger.warning("Unknown availability flag; skipping", flag_name=flag_name)
        return False
    try:
        conn.execute(
            "UPDATE player SET data_availability_flags = data_availability_flags | ? "
            "WHERE player_id = ?",
            (bit, player_id),
        )
        return True
    except sqlite3.Error as e:
        logger.warning(
            "Failed to set availability flag on player",
            player_id=player_id,
            flag_name=flag_name,
            error=str(e),
        )
        return False


# ---------------------------------------------------------------------------
# Row-Level Quarantine
# ---------------------------------------------------------------------------


def quarantine_row(
    quarantine_base_dir: str | Path,
    entity_type: str,
    entity_id: str,
    raw_row: dict[str, Any],
    reason: str,
) -> Path:
    """
    Write a single invalid/unresolvable row to the quarantine directory.

    Unlike BaseIngestor._quarantine_data() which writes a whole batch, this
    function quarantines individual rows that fail FK checks or field-level
    validation during the upsert loop.  Allows the pipeline to continue
    processing remaining rows.

    Args:
        quarantine_base_dir: Root quarantine directory (from settings.quarantine_dir).
        entity_type: Ingestor entity type string (used as subdirectory).
        entity_id: Identifier for the parent entity (game_id, player_id, etc.).
        raw_row: The individual row dict that could not be processed.
        reason: Human-readable explanation (e.g. "FK player.player_id=999 not found").

    Returns:
        Path to the written quarantine file.
    """
    base = Path(quarantine_base_dir)
    entity_dir = base / entity_type
    entity_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
    safe_id = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(entity_id))
    filepath = entity_dir / f"{timestamp}_{safe_id}.json"

    payload = {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "timestamp": datetime.now(UTC).isoformat(),
        "reason": reason,
        "row": raw_row,
    }

    try:
        with filepath.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=str, ensure_ascii=False)
        logger.debug("Row quarantined", entity_type=entity_type, path=str(filepath), reason=reason)
    except OSError as e:
        logger.warning("Failed to write quarantine row file", error=str(e))

    return filepath


# ---------------------------------------------------------------------------
# Audit helpers
# ---------------------------------------------------------------------------


def upsert_audit(
    conn: sqlite3.Connection,
    entity_type: str,
    entity_id: str,
    source: str,
    status: str,
    row_count: int = 0,
    error_message: str | None = None,
) -> None:
    """
    Insert or replace a row in ingestion_audit.

    Uses INSERT OR REPLACE so that re-running an ingestor updates the existing
    audit record rather than creating a duplicate (per the UNIQUE constraint on
    entity_type, entity_id, source).

    Args:
        conn: Open SQLite connection.
        entity_type: Ingestor entity type (e.g. "game_schedule").
        entity_id: Identifier for the ingested entity.
        source: Source system string (e.g. "nba_api", "kaggle_csv").
        status: "SUCCESS" | "FAILED" | "EMPTY" | "SKIPPED".
        row_count: Number of rows affected.
        error_message: Optional error detail for FAILED status.
    """
    try:
        conn.execute(
            """
            INSERT INTO ingestion_audit
                (entity_type, entity_id, source, ingest_ts, status, row_count, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(entity_type, entity_id, source)
            DO UPDATE SET
                ingest_ts     = excluded.ingest_ts,
                status        = excluded.status,
                row_count     = excluded.row_count,
                error_message = excluded.error_message
            """,
            (
                entity_type,
                str(entity_id),
                source,
                datetime.now(UTC).isoformat(),
                status,
                row_count,
                error_message,
            ),
        )
    except sqlite3.Error as e:
        logger.warning(
            "Failed to write audit record",
            entity_type=entity_type,
            entity_id=entity_id,
            error=str(e),
        )
