"""Shufinskiy pre-assembled NBA play-by-play and shot chart ingestor.

Source: shufinskiy/nba_data (Apache-2.0), updated weekly.
GitHub: https://github.com/shufinskiy/nba_data

Provides pre-downloaded tar.xz archives for every season from 1996-97 to
present. Each archive contains a CSV with data from one of four sources:
  nbastats   — stats.nba.com play-by-play (1996-present)
  shotdetail — stats.nba.com shot chart details (1996-present)
  pbpstats   — pbpstats.com possession-level data (2000-present)
  datanba    — data.nba.com play-by-play with coordinates (2016-present)

This ingestor downloads nbastats (→ play_by_play table) and shotdetail
(→ shot_chart table) for both regular season and playoffs, for all
available seasons. pbpstats data is loaded into the possession table.

Column mapping is derived from description_fields.md in the source repo.

Each season/type combination is tracked in ingestion_audit so the ingestor
is fully resumable — already-completed seasons are skipped.

Usage:
    ingestor = ShufinskiyPBPIngestor()
    # Ingest all available seasons
    result = ingestor.ingest("all", conn)
    # Ingest a specific season
    result = ingestor.ingest("2023", conn)
    # Ingest a specific season + type
    result = ingestor.ingest("2023_po", conn)  # playoffs
"""

from __future__ import annotations

import csv
import io
import sqlite3
import tarfile
import urllib.request
from typing import Any

import structlog

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.registry import register_ingestor
from nba_vault.ingestion.validation import upsert_audit

logger = structlog.get_logger(__name__)

_BASE_URL = "https://raw.githubusercontent.com/shufinskiy/nba_data/main/datasets/"
_LIST_URL = "https://raw.githubusercontent.com/shufinskiy/nba_data/main/list_data.txt"

# Seasons available for nbastats and shotdetail
_NBASTATS_START = 1996
_SHOTDETAIL_START = 1996
_PBPSTATS_START = 2000

# Current season (update annually)
_CURRENT_SEASON = 2024


def _available_seasons(start: int) -> list[int]:
    return list(range(start, _CURRENT_SEASON + 1))


@register_ingestor
class ShufinskiyPBPIngestor(BaseIngestor):
    """
    Bulk ingestor for pre-assembled NBA PBP + shot chart data (1996-present).

    Downloads tar.xz archives from shufinskiy/nba_data GitHub repo and
    loads them into play_by_play, shot_chart, and possession tables.

    entity_id convention:
        "all"        — all seasons, regular season + playoffs
        "YYYY"       — specific season (regular season only)
        "YYYY_po"    — specific season playoffs

    Usage:
        ingestor = ShufinskiyPBPIngestor()
        result = ingestor.ingest("all", conn)
    """

    entity_type = "shufinskiy_pbp"

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        """
        Fetch the list_data.txt manifest from the repo to resolve download URLs,
        then return the manifest. Actual CSV downloads happen in upsert() to
        avoid holding all data in memory simultaneously.
        """
        cache_key = "shufinskiy_manifest"
        cached = self.cache.get(cache_key)
        if cached:
            return cached  # type: ignore[return-value]

        self.logger.info("Fetching shufinskiy manifest", url=_LIST_URL)
        with urllib.request.urlopen(_LIST_URL, timeout=60) as resp:  # noqa: S310
            content = resp.read().decode("utf-8")

        # Parse "key=url" lines into a dict
        manifest: dict[str, str] = {}
        for raw_line in content.splitlines():
            line = raw_line.strip()
            if "=" in line:
                key, url = line.split("=", 1)
                manifest[key.strip()] = url.strip()

        payload: dict[str, Any] = {
            "manifest": manifest,
            "entity_id": entity_id,
        }
        self.cache.set(cache_key, payload)
        self.logger.info("Manifest loaded", entries=len(manifest))
        return payload

    def validate(self, raw: dict[str, Any]) -> list[Any]:
        # Validation is row-level inside upsert; return raw payload unchanged
        return [raw]

    def upsert(self, model: list[Any], conn: Any) -> int:
        if not model:
            return 0
        payload = model[0]
        manifest: dict[str, str] = payload.get("manifest", {})
        entity_id: str = payload.get("entity_id", "all")

        # Determine which (season, season_type) pairs to process
        targets = _resolve_targets(entity_id)

        total_rows = 0
        for season, season_type in targets:
            total_rows += self._ingest_season(conn, manifest, season, season_type)

        upsert_audit(conn, self.entity_type, entity_id, "shufinskiy_github", "SUCCESS", total_rows)
        return total_rows

    def _ingest_season(
        self,
        conn: Any,
        manifest: dict[str, str],
        season: int,
        season_type: str,
    ) -> int:
        """Ingest one season's PBP + shot chart data. Returns rows inserted."""
        suffix = "_po" if season_type == "po" else ""
        audit_id = f"{season}{suffix}"
        rows_total = 0

        # Check if already done
        if _already_done(conn, self.entity_type, audit_id):
            self.logger.debug("Season already ingested, skipping", season=audit_id)
            return 0

        self.logger.info("Ingesting shufinskiy season", season=audit_id)

        # --- nbastats (play_by_play) ---
        nbastats_key = f"nbastats{suffix}_{season}"
        if nbastats_key in manifest and season >= _NBASTATS_START:
            rows_total += self._load_nbastats(conn, manifest[nbastats_key], season, season_type)

        # --- shotdetail (shot_chart) ---
        shot_key = f"shotdetail{suffix}_{season}"
        if shot_key in manifest and season >= _SHOTDETAIL_START:
            rows_total += self._load_shotdetail(conn, manifest[shot_key], season, season_type)

        # --- pbpstats (possession) ---
        pbp_key = f"pbpstats{suffix}_{season}"
        if pbp_key in manifest and season >= _PBPSTATS_START:
            rows_total += self._load_pbpstats(conn, manifest[pbp_key], season, season_type)

        upsert_audit(conn, self.entity_type, audit_id, "shufinskiy_github", "SUCCESS", rows_total)
        self.logger.info("Season ingested", season=audit_id, rows=rows_total)
        return rows_total

    def _download_csv(self, url: str) -> list[dict[str, str]]:
        """Download a tar.xz archive and extract the single CSV inside."""
        self.logger.debug("Downloading archive", url=url)
        with urllib.request.urlopen(url, timeout=300) as resp:  # noqa: S310
            data = resp.read()

        with tarfile.open(fileobj=io.BytesIO(data), mode="r:xz") as tar:
            csv_members = [m for m in tar.getmembers() if m.name.endswith(".csv")]
            if not csv_members:
                self.logger.warning("No CSV found in archive", url=url)
                return []
            f = tar.extractfile(csv_members[0])
            if f is None:
                return []
            content = f.read().decode("utf-8")

        reader = csv.DictReader(io.StringIO(content))
        return list(reader)

    def _load_nbastats(self, conn: Any, url: str, season: int, season_type: str) -> int:
        """Load nbastats CSV rows into play_by_play table."""
        try:
            rows = self._download_csv(url)
        except Exception as e:
            self.logger.warning("Failed to download nbastats", url=url, error=str(e))
            return 0

        inserted = 0
        conn.execute("BEGIN")
        try:
            for row in rows:
                game_id = row.get("GAME_ID", "").strip()
                event_num = _safe_int(row.get("EVENTNUM"))
                period = _safe_int(row.get("PERIOD"))
                if not game_id or event_num is None or period is None:
                    continue

                # Only insert if the game exists in the game table
                if not _game_exists(conn, game_id):
                    continue

                pc_time_str = row.get("PCTIMESTRING", "").strip()
                pc_time = _parse_pc_time(pc_time_str)

                conn.execute(
                    """
                    INSERT INTO play_by_play
                        (game_id, event_num, period, pc_time, wc_time,
                         event_type, event_action_type,
                         description_home, description_visitor,
                         score_home, score_visitor, score_margin,
                         player1_id, player1_team_id,
                         player2_id, player2_team_id,
                         player3_id, player3_team_id,
                         video_available)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(game_id, event_num) DO UPDATE SET
                        period             = excluded.period,
                        pc_time            = excluded.pc_time,
                        wc_time            = excluded.wc_time,
                        event_type         = excluded.event_type,
                        event_action_type  = excluded.event_action_type,
                        description_home   = excluded.description_home,
                        description_visitor = excluded.description_visitor,
                        score_home         = excluded.score_home,
                        score_visitor      = excluded.score_visitor,
                        score_margin       = excluded.score_margin,
                        player1_id         = excluded.player1_id,
                        player1_team_id    = excluded.player1_team_id,
                        player2_id         = excluded.player2_id,
                        player2_team_id    = excluded.player2_team_id,
                        player3_id         = excluded.player3_id,
                        player3_team_id    = excluded.player3_team_id,
                        video_available    = excluded.video_available
                    """,
                    (
                        game_id,
                        event_num,
                        period,
                        pc_time,
                        row.get("WCTIMESTRING", "").strip() or None,
                        _safe_int(row.get("EVENTMSGTYPE")),
                        _safe_int(row.get("EVENTMSGACTIONTYPE")),
                        row.get("HOMEDESCRIPTION", "").strip() or None,
                        row.get("VISITORDESCRIPTION", "").strip() or None,
                        _parse_score(row.get("SCORE", ""), "home"),
                        _parse_score(row.get("SCORE", ""), "visitor"),
                        _safe_int(row.get("SCOREMARGIN")),
                        _safe_int(row.get("PLAYER1_ID")),
                        _safe_int(row.get("PLAYER1_TEAM_ID")),
                        _safe_int(row.get("PLAYER2_ID")),
                        _safe_int(row.get("PLAYER2_TEAM_ID")),
                        _safe_int(row.get("PLAYER3_ID")),
                        _safe_int(row.get("PLAYER3_TEAM_ID")),
                        1 if row.get("VIDEO_AVAILABLE_FLAG", "0") == "1" else 0,
                    ),
                )
                inserted += 1
            conn.execute("COMMIT")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        self.logger.debug("Loaded nbastats rows", season=season, type=season_type, rows=inserted)
        return inserted

    def _load_shotdetail(self, conn: Any, url: str, season: int, season_type: str) -> int:
        """Load shotdetail CSV rows into shot_chart table."""
        try:
            rows = self._download_csv(url)
        except Exception as e:
            self.logger.warning("Failed to download shotdetail", url=url, error=str(e))
            return 0

        inserted = 0
        conn.execute("BEGIN")
        try:
            for row in rows:
                game_id = row.get("GAME_ID", "").strip()
                player_id = _safe_int(row.get("PLAYER_ID"))
                team_id = _safe_int(row.get("TEAM_ID"))
                period = _safe_int(row.get("PERIOD"))
                if not game_id or player_id is None or period is None:
                    continue

                if not _game_exists(conn, game_id):
                    continue

                conn.execute(
                    """
                    INSERT INTO shot_chart
                        (game_id, player_id, team_id, period,
                         minutes_remaining, seconds_remaining,
                         action_type, shot_type,
                         shot_zone_basic, shot_zone_area, shot_zone_range,
                         shot_distance, loc_x, loc_y,
                         shot_made_flag, htm, vtm)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        game_id,
                        player_id,
                        team_id,
                        period,
                        _safe_int(row.get("MINUTES_REMAINING")),
                        _safe_int(row.get("SECONDS_REMAINING")),
                        row.get("ACTION_TYPE", "").strip() or None,
                        row.get("SHOT_TYPE", "").strip() or None,
                        row.get("SHOT_ZONE_BASIC", "").strip() or None,
                        row.get("SHOT_ZONE_AREA", "").strip() or None,
                        row.get("SHOT_ZONE_RANGE", "").strip() or None,
                        _safe_int(row.get("SHOT_DISTANCE")),
                        _safe_int(row.get("LOC_X")),
                        _safe_int(row.get("LOC_Y")),
                        1 if row.get("SHOT_MADE_FLAG", "0") == "1" else 0,
                        row.get("HTM", "").strip() or None,
                        row.get("VTM", "").strip() or None,
                    ),
                )
                inserted += 1
            conn.execute("COMMIT")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        self.logger.debug("Loaded shotdetail rows", season=season, type=season_type, rows=inserted)
        return inserted

    def _load_pbpstats(self, conn: Any, url: str, season: int, season_type: str) -> int:
        """Load pbpstats CSV rows into possession table."""
        try:
            rows = self._download_csv(url)
        except Exception as e:
            self.logger.warning("Failed to download pbpstats", url=url, error=str(e))
            return 0

        inserted = 0
        conn.execute("BEGIN")
        try:
            for i, row in enumerate(rows):
                game_id = row.get("GAMEID", "").strip()
                period = _safe_int(row.get("PERIOD"))
                if not game_id or period is None:
                    continue

                if not _game_exists(conn, game_id):
                    continue

                start_time = _parse_time_str(row.get("STARTTIME", ""))
                end_time = _parse_time_str(row.get("ENDTIME", ""))
                if start_time is None:
                    continue

                # possession_number within game derived from row order per game
                conn.execute(
                    """
                    INSERT INTO possession
                        (game_id, possession_number, period,
                         start_time, end_time,
                         points_scored, play_type, outcome_type)
                    VALUES (?,?,?,?,?,?,?,?)
                    ON CONFLICT(game_id, possession_number) DO UPDATE SET
                        period        = excluded.period,
                        start_time    = excluded.start_time,
                        end_time      = excluded.end_time,
                        points_scored = excluded.points_scored,
                        play_type     = excluded.play_type,
                        outcome_type  = excluded.outcome_type
                    """,
                    (
                        game_id,
                        i + 1,  # approximate possession number within the CSV slice
                        period,
                        start_time,
                        end_time,
                        (_safe_int(row.get("FG2M", "0")) or 0) * 2
                        + (_safe_int(row.get("FG3M", "0")) or 0) * 3,
                        row.get("STARTTYPE", "").strip() or None,
                        "turnover" if (_safe_int(row.get("TURNOVERS", "0")) or 0) > 0 else None,
                    ),
                )
                inserted += 1
            conn.execute("COMMIT")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        self.logger.debug("Loaded pbpstats rows", season=season, type=season_type, rows=inserted)
        return inserted


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_targets(entity_id: str) -> list[tuple[int, str]]:
    """Return list of (season_year, season_type) pairs to process."""
    if entity_id == "all":
        targets = []
        for s in _available_seasons(_NBASTATS_START):
            targets.append((s, "rg"))
            targets.append((s, "po"))
        return targets
    if entity_id.endswith("_po"):
        season = _safe_int(entity_id[:-3])
        return [(season, "po")] if season else []
    season = _safe_int(entity_id)
    return [(season, "rg")] if season else []


def _already_done(conn: Any, entity_type: str, entity_id: str) -> bool:
    try:
        row = conn.execute(
            "SELECT 1 FROM ingestion_audit "
            "WHERE entity_type=? AND entity_id=? AND status='SUCCESS' LIMIT 1",
            (entity_type, entity_id),
        ).fetchone()
        return row is not None
    except sqlite3.Error:
        return False


def _game_exists(conn: Any, game_id: str) -> bool:
    try:
        row = conn.execute("SELECT 1 FROM game WHERE game_id=? LIMIT 1", (game_id,)).fetchone()
        return row is not None
    except sqlite3.Error:
        return False


def _parse_pc_time(time_str: str) -> int | None:
    """Convert 'MM:SS' period clock string to total seconds remaining."""
    if not time_str:
        return None
    parts = time_str.split(":")
    if len(parts) == 2:
        try:
            return int(parts[0]) * 60 + int(parts[1])
        except ValueError:
            return None
    return None


def _parse_time_str(time_str: str) -> float | None:
    """Convert 'MM:SS' or decimal string to float seconds."""
    if not time_str:
        return None
    time_str = time_str.strip()
    if ":" in time_str:
        parts = time_str.split(":")
        try:
            return float(parts[0]) * 60 + float(parts[1])
        except (ValueError, IndexError):
            return None
    try:
        return float(time_str)
    except ValueError:
        return None


def _parse_score(score_str: str, side: str) -> int | None:
    """Parse 'HHH - VVV' score string."""
    if not score_str or "-" not in score_str:
        return None
    parts = score_str.split("-")
    if len(parts) != 2:
        return None
    try:
        if side == "home":
            return int(parts[0].strip())
        return int(parts[1].strip())
    except ValueError:
        return None


def _safe_int(val: Any) -> int | None:
    if val is None or str(val).strip() in ("", "None", "null", "NA", "0.0"):
        return None
    try:
        result = int(float(str(val).strip()))
        return result if result != 0 else None
    except (ValueError, TypeError):
        return None
