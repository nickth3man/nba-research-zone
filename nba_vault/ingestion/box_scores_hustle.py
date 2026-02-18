"""Hustle box score ingestor.

Source: nba_api BoxScoreHustleV2 endpoint.
Populates `player_game_log_hustle` table.
Era gate: available from 2015-16 season onwards.
"""

from __future__ import annotations

import sqlite3
from typing import Any, cast

import pydantic
import structlog

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.nba_stats_client import NBAStatsClient
from nba_vault.ingestion.registry import register_ingestor
from nba_vault.ingestion.validation import (
    check_data_availability,
    require_fk,
    set_game_availability_flag,
    upsert_audit,
)
from nba_vault.models.entities import BoxScoreHustleRowCreate

logger = structlog.get_logger(__name__)

_PLAYER_STATS_DATASET = "PlayerStats"


def _i(val: Any) -> int | None:
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _f(val: Any) -> float | None:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


@register_ingestor
class BoxScoreHustleIngestor(BaseIngestor):
    """
    Ingestor for hustle-stat box scores (2015-16+).

    entity_id convention: "<game_id>" e.g. "0022300001"
    kwargs:
        season_year (int): Integer start year of season e.g. 2023

    Usage:
        ingestor = BoxScoreHustleIngestor()
        result = ingestor.ingest("0022300001", conn, season_year=2023)
    """

    entity_type = "box_score_hustle"

    def __init__(self, cache: Any = None, rate_limiter: Any = None) -> None:
        super().__init__(cache, rate_limiter)
        self._client = NBAStatsClient(cache=self.cache, rate_limiter=self.rate_limiter)  # type: ignore[arg-type]

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        game_id = entity_id
        season_year: int = int(kwargs.get("season_year", 0))
        # Era gate: hustle stats only available 2015-16+
        check_data_availability("hustle_stats", season_year)

        cache_key = f"box_score_hustle_{game_id}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached  # type: ignore[return-value]

        self.rate_limiter.acquire()
        self.logger.info("Fetching hustle box score", game_id=game_id)
        raw = self._client.adapter.get_box_score_hustle(game_id=game_id)

        ds = raw.get(_PLAYER_STATS_DATASET, {})
        hdrs: list[str] = ds.get("headers", [])
        data: list[list[Any]] = ds.get("data", [])
        player_rows = [dict(zip(hdrs, row, strict=False)) for row in data]

        payload: dict[str, Any] = {
            "player_rows": player_rows,
            "game_id": game_id,
            "season_year": season_year,
        }
        self.cache.set(cache_key, payload)
        self.logger.info("Fetched hustle rows", game_id=game_id, count=len(player_rows))
        return payload

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        game_id = raw.get("game_id", "")
        validated: list[pydantic.BaseModel] = []
        for row in raw.get("player_rows", []):
            try:
                model = BoxScoreHustleRowCreate(
                    game_id=game_id,
                    player_id=int(row.get("PLAYER_ID", 0)),
                    team_id=int(row.get("TEAM_ID", 0)),
                    minutes_played=_f(row.get("MIN")),
                    contested_shots=_i(row.get("CONTESTED_SHOTS")),
                    contested_shots_2pt=_i(row.get("CONTESTED_SHOTS_2PT")),
                    contested_shots_3pt=_i(row.get("CONTESTED_SHOTS_3PT")),
                    deflections=_i(row.get("DEFLECTIONS")),
                    charges_drawn=_i(row.get("CHARGES_DRAWN")),
                    screen_assists=_i(row.get("SCREEN_ASSISTS")),
                    screen_ast_pts=_i(row.get("SCREEN_AST_PTS")),
                    box_outs=_i(row.get("BOX_OUTS")),
                    off_box_outs=_i(row.get("OFF_BOXOUTS")),
                    def_box_outs=_i(row.get("DEF_BOXOUTS")),
                    loose_balls_recovered=_i(row.get("LOOSE_BALLS_RECOVERED")),
                )
                validated.append(model)
            except (pydantic.ValidationError, ValueError) as exc:
                self.logger.warning(
                    "Hustle row validation failed",
                    game_id=game_id,
                    player_id=row.get("PLAYER_ID"),
                    error=str(exc),
                )
        return validated

    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        game_id: str = ""
        rows_affected = 0
        conn.execute("BEGIN")
        try:
            for item in model:
                h = cast("BoxScoreHustleRowCreate", item)
                game_id = game_id or h.game_id
                if not require_fk(conn, "game", "game_id", h.game_id):
                    continue
                conn.execute(
                    """
                    INSERT INTO player_game_log_hustle
                        (game_id, player_id, team_id, minutes_played,
                         contested_shots, contested_shots_2pt, contested_shots_3pt,
                         deflections, charges_drawn,
                         screen_assists, screen_ast_pts,
                         box_outs, off_box_outs, def_box_outs,
                         loose_balls_recovered)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(game_id, player_id) DO UPDATE SET
                        minutes_played        = excluded.minutes_played,
                        contested_shots       = excluded.contested_shots,
                        deflections           = excluded.deflections,
                        charges_drawn         = excluded.charges_drawn,
                        screen_assists        = excluded.screen_assists,
                        box_outs              = excluded.box_outs,
                        loose_balls_recovered = excluded.loose_balls_recovered
                    """,
                    (
                        h.game_id,
                        h.player_id,
                        h.team_id,
                        h.minutes_played,
                        h.contested_shots,
                        h.contested_shots_2pt,
                        h.contested_shots_3pt,
                        h.deflections,
                        h.charges_drawn,
                        h.screen_assists,
                        h.screen_ast_pts,
                        h.box_outs,
                        h.off_box_outs,
                        h.def_box_outs,
                        h.loose_balls_recovered,
                    ),
                )
                rows_affected += 1
            conn.execute("COMMIT")
            if game_id:
                set_game_availability_flag(conn, game_id, "hustle_stats")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        upsert_audit(conn, self.entity_type, game_id or "all", "nba_api", "SUCCESS", rows_affected)
        self.logger.info("Upserted hustle box score rows", rows_affected=rows_affected)
        return rows_affected
