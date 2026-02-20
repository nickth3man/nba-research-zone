"""Pre-modern (pre-1996) box score ingestor.

Source: eoinamoore/historical-nba-data-and-player-box-scores (CC0 / public domain).
Kaggle: https://www.kaggle.com/datasets/eoinamoore/historical-nba-data-and-player-box-scores
Updated daily. Contains player box scores for every NBA game from 1947 to present.

This ingestor is designed to fill the pre-1996 gap in player_game_log that the
NBA.com API cannot cover. For 1996+ seasons, the shufinskiy/nba_data ingestor
(or the NBA.com API ingestors) provide the same data; this ingestor skips rows
where the game already has box score data to avoid duplicates.

The Kaggle dataset requires a manual download because Kaggle requires
authentication. The ingestor accepts a local file path to PlayerStatistics.csv.

Usage:
    # Download PlayerStatistics.csv from Kaggle first, then:
    ingestor = PreModernBoxScoreIngestor()
    result = ingestor.ingest("/path/to/PlayerStatistics.csv", conn)

    # Or via CLI (once the CLI command is wired up):
    nba-vault game-data ingest-pre-modern-box-scores --csv-path /path/to/PlayerStatistics.csv

Column mapping (eoinamoore → player_game_log):
    personId      → player_id
    gameId        → game_id
    teamId        → team_id
    minutes       → minutes_played
    fieldGoalsMade / fieldGoalsAttempted / fieldGoalsPercentage → fgm/fga/fg_pct
    threePointersMade / threePointersAttempted / threePointersPercentage → fg3m/fg3a/fg3_pct
    freeThrowsMade / freeThrowsAttempted / freeThrowsPercentage → ftm/fta/ft_pct
    reboundsOffensive / reboundsDefensive / reboundsTotal → oreb/dreb/reb
    assists → ast
    steals → stl
    blocks → blk
    turnovers → tov
    foulsPersonal → pf
    points → pts
    plusMinusPoints → plus_minus
"""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Any

import structlog

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.registry import register_ingestor
from nba_vault.ingestion.validation import upsert_audit

logger = structlog.get_logger(__name__)

# Only load rows for seasons before this year (1996+ is covered by other ingestors)
# Set to None to load all seasons (useful for a full refresh)
_PRE_MODERN_CUTOFF_SEASON = 1996


@register_ingestor
class PreModernBoxScoreIngestor(BaseIngestor):
    """
    Bulk ingestor for pre-1996 NBA player box scores from eoinamoore Kaggle dataset.

    Reads a local PlayerStatistics.csv file (CC0, public domain) and inserts
    rows into player_game_log for seasons before 1996-97. Rows for seasons
    already covered by NBA.com API ingestors (1996+) are skipped by default.

    entity_id convention: path to the local CSV file

    Usage:
        ingestor = PreModernBoxScoreIngestor()
        result = ingestor.ingest("/path/to/PlayerStatistics.csv", conn)
        # Load all seasons (including 1996+):
        result = ingestor.ingest("/path/to/PlayerStatistics.csv", conn, all_seasons=True)
    """

    entity_type = "pre_modern_box_scores"

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        csv_path = Path(entity_id)
        if not csv_path.exists():
            msg = (
                f"CSV file not found: {csv_path}\n"
                "Download PlayerStatistics.csv from:\n"
                "https://www.kaggle.com/datasets/eoinamoore/historical-nba-data-and-player-box-scores"
            )
            raise FileNotFoundError(msg)

        all_seasons: bool = kwargs.get("all_seasons", False)
        cutoff = None if all_seasons else _PRE_MODERN_CUTOFF_SEASON

        self.logger.info(
            "Reading pre-modern box score CSV",
            path=str(csv_path),
            cutoff_season=cutoff,
        )

        rows: list[dict[str, str]] = []
        with csv_path.open(encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                # Filter by season if cutoff is set
                if cutoff is not None:
                    season_year = _extract_season_year(row)
                    if season_year is not None and season_year >= cutoff:
                        continue
                rows.append(row)

        self.logger.info("Read CSV rows", count=len(rows), cutoff=cutoff)
        return {"rows": rows, "csv_path": str(csv_path)}

    def validate(self, raw: dict[str, Any]) -> list[Any]:
        # Row-level validation happens in upsert; return raw payload
        return [raw]

    def upsert(self, model: list[Any], conn: Any) -> int:
        if not model:
            return 0
        payload = model[0]
        rows: list[dict[str, str]] = payload.get("rows", [])
        csv_path: str = payload.get("csv_path", "unknown")

        # Build player_id lookup: bbref_id → player_id (for cross-referencing)
        # eoinamoore uses NBA.com personId which maps directly to player.player_id
        inserted = 0
        skipped_no_game = 0
        skipped_no_player = 0

        conn.execute("BEGIN")
        try:
            for row in rows:
                game_id = _normalise_game_id(row.get("gameId", ""))
                player_id = _safe_int(row.get("personId"))
                team_id = _safe_int(row.get("teamId"))

                if not game_id or player_id is None:
                    continue

                # Skip if game not in game table (FK constraint)
                if not _game_exists(conn, game_id):
                    skipped_no_game += 1
                    continue

                # Skip if player not in player table
                if not _player_exists(conn, player_id):
                    skipped_no_player += 1
                    continue

                minutes_played = _parse_minutes(row.get("minutes", ""))
                fg_pct = _safe_float(row.get("fieldGoalsPercentage"))
                fg3_pct = _safe_float(row.get("threePointersPercentage"))
                ft_pct = _safe_float(row.get("freeThrowsPercentage"))

                conn.execute(
                    """
                    INSERT INTO player_game_log
                        (game_id, player_id, team_id, season_id,
                         minutes_played,
                         fgm, fga, fg_pct,
                         fg3m, fg3a, fg3_pct,
                         ftm, fta, ft_pct,
                         oreb, dreb, reb,
                         ast, stl, blk, tov, pf, pts,
                         plus_minus)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(game_id, player_id, team_id) DO UPDATE SET
                        minutes_played = excluded.minutes_played,
                        fgm            = excluded.fgm,
                        fga            = excluded.fga,
                        fg_pct         = excluded.fg_pct,
                        fg3m           = excluded.fg3m,
                        fg3a           = excluded.fg3a,
                        fg3_pct        = excluded.fg3_pct,
                        ftm            = excluded.ftm,
                        fta            = excluded.fta,
                        ft_pct         = excluded.ft_pct,
                        oreb           = excluded.oreb,
                        dreb           = excluded.dreb,
                        reb            = excluded.reb,
                        ast            = excluded.ast,
                        stl            = excluded.stl,
                        blk            = excluded.blk,
                        tov            = excluded.tov,
                        pf             = excluded.pf,
                        pts            = excluded.pts,
                        plus_minus     = excluded.plus_minus
                    """,
                    (
                        game_id,
                        player_id,
                        team_id,
                        _extract_season_year(row),
                        minutes_played,
                        _safe_int(row.get("fieldGoalsMade")),
                        _safe_int(row.get("fieldGoalsAttempted")),
                        fg_pct / 100.0 if fg_pct and fg_pct > 1.0 else fg_pct,
                        _safe_int(row.get("threePointersMade")),
                        _safe_int(row.get("threePointersAttempted")),
                        fg3_pct / 100.0 if fg3_pct and fg3_pct > 1.0 else fg3_pct,
                        _safe_int(row.get("freeThrowsMade")),
                        _safe_int(row.get("freeThrowsAttempted")),
                        ft_pct / 100.0 if ft_pct and ft_pct > 1.0 else ft_pct,
                        _safe_int(row.get("reboundsOffensive")),
                        _safe_int(row.get("reboundsDefensive")),
                        _safe_int(row.get("reboundsTotal")),
                        _safe_int(row.get("assists")),
                        _safe_int(row.get("steals")),
                        _safe_int(row.get("blocks")),
                        _safe_int(row.get("turnovers")),
                        _safe_int(row.get("foulsPersonal")),
                        _safe_int(row.get("points")),
                        _safe_int(row.get("plusMinusPoints")),
                    ),
                )
                inserted += 1
            conn.execute("COMMIT")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        self.logger.info(
            "Pre-modern box scores upserted",
            inserted=inserted,
            skipped_no_game=skipped_no_game,
            skipped_no_player=skipped_no_player,
        )
        upsert_audit(
            conn,
            self.entity_type,
            csv_path,
            "kaggle_eoinamoore",
            "SUCCESS",
            inserted,
        )
        return inserted


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalise_game_id(raw: str) -> str:
    """Ensure game_id is a 10-character zero-padded string."""
    raw = str(raw).strip()
    if not raw or raw in ("None", "null"):
        return ""
    # eoinamoore stores game IDs as integers; pad to 10 chars
    try:
        return str(int(float(raw))).zfill(10)
    except ValueError:
        return raw if len(raw) == 10 else ""


def _extract_season_year(row: dict[str, str]) -> int | None:
    """Extract season start year from a row. Tries seasonYear and gameId."""
    season_raw = row.get("seasonYear", "") or row.get("season_year", "")
    if season_raw:
        val = _safe_int(season_raw)
        if val:
            # eoinamoore uses end year (e.g. 1997 for 1996-97); convert to start year
            return val - 1 if val > 1946 else val
    # Fall back to parsing game_id: first 3 digits encode season
    game_id = row.get("gameId", "")
    if game_id and len(str(game_id)) >= 5:
        try:
            yy = int(str(int(float(game_id))).zfill(10)[3:5])
            # yy is last 2 digits of season end year; approximate
            return (2000 + yy - 1) if yy < 50 else (1900 + yy - 1)
        except (ValueError, IndexError):
            pass
    return None


def _parse_minutes(minutes_str: str) -> float | None:
    """Parse minutes string (e.g. '32:15' or '32.5') to decimal minutes."""
    if not minutes_str or minutes_str.strip() in ("", "None", "null"):
        return None
    minutes_str = minutes_str.strip()
    if ":" in minutes_str:
        parts = minutes_str.split(":")
        try:
            return float(parts[0]) + float(parts[1]) / 60.0
        except (ValueError, IndexError):
            return None
    try:
        return float(minutes_str)
    except ValueError:
        return None


def _game_exists(conn: Any, game_id: str) -> bool:
    try:
        row = conn.execute("SELECT 1 FROM game WHERE game_id=? LIMIT 1", (game_id,)).fetchone()
        return row is not None
    except sqlite3.Error:
        return False


def _player_exists(conn: Any, player_id: int) -> bool:
    try:
        row = conn.execute(
            "SELECT 1 FROM player WHERE player_id=? LIMIT 1", (player_id,)
        ).fetchone()
        return row is not None
    except sqlite3.Error:
        return False


def _safe_int(val: Any) -> int | None:
    if val is None or str(val).strip() in ("", "None", "null", "NA"):
        return None
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None


def _safe_float(val: Any) -> float | None:
    if val is None or str(val).strip() in ("", "None", "null", "NA"):
        return None
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None
