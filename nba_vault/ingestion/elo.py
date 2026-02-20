"""ELO ratings ingestor.

Source: Neil-Paine-1/NBA-elo (MIT license, updated 2025), which is an
actively maintained fork of the original fivethirtyeight/data nba-elo dataset
(CC BY 4.0). Covers every NBA/ABA/BAA game from November 1946 to present.

The CSV has one row per team per game (two rows per game). The `_iscopy`
column flags the duplicate row; we filter to _iscopy == 0 to get one row
per team per game (the "primary" row for each team).

Populates the `game_elo` table.

Usage:
    ingestor = EloIngestor()
    result = ingestor.ingest("all", conn)
"""

from __future__ import annotations

import csv
import io
import sqlite3
import urllib.request
from typing import Any, cast

import pydantic
import structlog

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.registry import register_ingestor
from nba_vault.ingestion.validation import upsert_audit
from nba_vault.models.entities import GameEloCreate

logger = structlog.get_logger(__name__)

_ELO_URL = "https://raw.githubusercontent.com/Neil-Paine-1/NBA-elo/main/nbaallelo.csv"
_FALLBACK_URL = (
    "https://raw.githubusercontent.com/fivethirtyeight/data/master/nba-elo/nbaallelo.csv"
)


@register_ingestor
class EloIngestor(BaseIngestor):
    """
    Ingestor for FiveThirtyEight/Neil-Paine ELO ratings (1946-present).

    Downloads the full nbaallelo.csv in one HTTP request (~17 MB) and
    bulk-inserts into game_elo. Idempotent via ON CONFLICT DO UPDATE.

    entity_id convention: "all" (single bulk load)

    Usage:
        ingestor = EloIngestor()
        result = ingestor.ingest("all", conn)
    """

    entity_type = "elo_ratings"

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        cache_key = "elo_nbaallelo_csv"
        cached = self.cache.get(cache_key)
        if cached:
            self.logger.debug("Cache hit for ELO CSV")
            return cached  # type: ignore[return-value]

        self.logger.info("Downloading ELO CSV", url=_ELO_URL)
        try:
            with urllib.request.urlopen(_ELO_URL, timeout=120) as resp:  # noqa: S310
                content = resp.read().decode("utf-8")
        except Exception as e:
            self.logger.warning(
                "Primary ELO URL failed, trying fallback",
                error=str(e),
                fallback=_FALLBACK_URL,
            )
            with urllib.request.urlopen(_FALLBACK_URL, timeout=120) as resp:  # noqa: S310
                content = resp.read().decode("utf-8")

        reader = csv.DictReader(io.StringIO(content))
        rows = [row for row in reader if row.get("_iscopy", "1") == "0"]
        self.logger.info("Downloaded ELO rows", count=len(rows))

        payload: dict[str, Any] = {"rows": rows}
        self.cache.set(cache_key, payload)
        return payload

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        validated: list[pydantic.BaseModel] = []
        for row in raw.get("rows", []):
            try:
                # Convert date from M/D/YYYY or YYYY-MM-DD to ISO YYYY-MM-DD
                game_date = _normalise_date(row.get("date_game", ""))
                if not game_date:
                    continue

                year_id = _safe_int(row.get("year_id"))
                # year_id is the season end year (e.g. 1947 for 1946-47)
                season_id = (year_id - 1) if year_id else None

                model = GameEloCreate(
                    game_date=game_date,
                    season_id=season_id,
                    bbref_team_id=row.get("team_id", "").strip() or None,
                    elo_before=float(row["elo_i"]),
                    elo_after=float(row["elo_n"]),
                    win_prob=_safe_float(row.get("forecast")),
                    win_equiv=_safe_float(row.get("win_equiv")),
                    opponent_elo=_safe_float(row.get("opp_elo_i")),
                    game_location=row.get("game_location", "").strip() or None,
                    pts_scored=_safe_int(row.get("pts")),
                    pts_allowed=_safe_int(row.get("opp_pts")),
                    game_result=row.get("game_result", "").strip() or None,
                    is_playoffs=1 if row.get("is_playoffs", "0") == "1" else 0,
                    notes=row.get("notes", "").strip() or None,
                    source="neil_paine_elo",
                )
                validated.append(model)
            except (pydantic.ValidationError, ValueError, KeyError) as exc:
                self.logger.debug("ELO row validation error", error=str(exc))
        self.logger.info("Validated ELO rows", count=len(validated))
        return validated

    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        # Build set of valid season_ids to avoid FK constraint failures when
        # seasons table is not yet populated (Stage 0 runs before Stage 1).
        try:
            valid_seasons: set[int] = {
                r[0] for r in conn.execute("SELECT season_id FROM season").fetchall()
            }
        except sqlite3.Error:
            valid_seasons = set()

        rows_affected = 0
        conn.execute("BEGIN")
        try:
            for item in model:
                e = cast("GameEloCreate", item)
                # Null out season_id if the season row doesn't exist yet
                season_id = e.season_id if e.season_id in valid_seasons else None
                conn.execute(
                    """
                    INSERT INTO game_elo
                        (game_date, season_id, bbref_team_id, elo_before, elo_after,
                         win_prob, win_equiv, opponent_elo, game_location,
                         pts_scored, pts_allowed, game_result, is_playoffs, notes, source)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(game_date, bbref_team_id) DO UPDATE SET
                        elo_before    = excluded.elo_before,
                        elo_after     = excluded.elo_after,
                        win_prob      = excluded.win_prob,
                        win_equiv     = excluded.win_equiv,
                        opponent_elo  = excluded.opponent_elo,
                        game_location = excluded.game_location,
                        pts_scored    = excluded.pts_scored,
                        pts_allowed   = excluded.pts_allowed,
                        game_result   = excluded.game_result,
                        is_playoffs   = excluded.is_playoffs,
                        notes         = excluded.notes,
                        source        = excluded.source
                    """,
                    (
                        e.game_date,
                        season_id,
                        e.bbref_team_id,
                        e.elo_before,
                        e.elo_after,
                        e.win_prob,
                        e.win_equiv,
                        e.opponent_elo,
                        e.game_location,
                        e.pts_scored,
                        e.pts_allowed,
                        e.game_result,
                        e.is_playoffs,
                        e.notes,
                        e.source,
                    ),
                )
                rows_affected += 1
            conn.execute("COMMIT")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        upsert_audit(conn, self.entity_type, "all", "neil_paine_elo", "SUCCESS", rows_affected)
        self.logger.info("Upserted ELO rows", rows_affected=rows_affected)
        return rows_affected


def _normalise_date(raw: str) -> str | None:
    """Convert various date formats to ISO YYYY-MM-DD."""
    raw = raw.strip()
    if not raw:
        return None
    # Already ISO
    if len(raw) == 10 and raw[4] == "-":
        return raw
    # M/D/YYYY
    parts = raw.split("/")
    if len(parts) == 3:
        try:
            m, d, y = int(parts[0]), int(parts[1]), int(parts[2])
            return f"{y:04d}-{m:02d}-{d:02d}"
        except ValueError:
            return None
    return None


def _safe_float(val: Any) -> float | None:
    if val is None or str(val).strip() in ("", "None", "null", "NA"):
        return None
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None


def _safe_int(val: Any) -> int | None:
    if val is None or str(val).strip() in ("", "None", "null", "NA"):
        return None
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None
