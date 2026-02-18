"""Advanced box score ingestor.

Source: nba_api BoxScoreAdvancedV2 endpoint.
Populates `player_game_log_advanced` table.
Available for NBA.com era games (approx. 1996-97+).
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
from nba_vault.models.entities import BoxScoreAdvancedRowCreate

logger = structlog.get_logger(__name__)

_PLAYER_STATS_DATASET = "PlayerStats"


def _f(val: Any) -> float | None:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


@register_ingestor
class BoxScoreAdvancedIngestor(BaseIngestor):
    """
    Ingestor for advanced per-game box scores (player rows).

    entity_id convention: "<game_id>" e.g. "0022300001"
    kwargs:
        season_year (int): Integer start year of season e.g. 2023

    Usage:
        ingestor = BoxScoreAdvancedIngestor()
        result = ingestor.ingest("0022300001", conn, season_year=2023)
    """

    entity_type = "box_score_advanced"

    def __init__(self, cache: Any = None, rate_limiter: Any = None) -> None:
        super().__init__(cache, rate_limiter)
        self._client = NBAStatsClient(cache=self.cache, rate_limiter=self.rate_limiter)  # type: ignore[arg-type]

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        game_id = entity_id
        season_year: int = int(kwargs.get("season_year", 0))
        check_data_availability("box_score_advanced", season_year)

        cache_key = f"box_score_advanced_{game_id}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached  # type: ignore[return-value]

        self.rate_limiter.acquire()
        self.logger.info("Fetching advanced box score", game_id=game_id)
        raw = self._client.adapter.get_box_score_advanced(game_id=game_id)

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
        self.logger.info("Fetched advanced rows", game_id=game_id, count=len(player_rows))
        return payload

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        game_id = raw.get("game_id", "")
        validated: list[pydantic.BaseModel] = []
        for row in raw.get("player_rows", []):
            try:
                model = BoxScoreAdvancedRowCreate(
                    game_id=game_id,
                    player_id=int(row.get("PLAYER_ID", 0)),
                    team_id=int(row.get("TEAM_ID", 0)),
                    minutes_played=_f(row.get("MIN")),
                    off_rating=_f(row.get("OFF_RATING")),
                    def_rating=_f(row.get("DEF_RATING")),
                    net_rating=_f(row.get("NET_RATING")),
                    ast_pct=_f(row.get("AST_PCT")),
                    ast_to_tov=_f(row.get("AST_TO")),
                    ast_ratio=_f(row.get("AST_RATIO")),
                    oreb_pct=_f(row.get("OREB_PCT")),
                    dreb_pct=_f(row.get("DREB_PCT")),
                    reb_pct=_f(row.get("REB_PCT")),
                    tov_pct=_f(row.get("TM_TOV_PCT")),
                    efg_pct=_f(row.get("EFG_PCT")),
                    ts_pct=_f(row.get("TS_PCT")),
                    usg_pct=_f(row.get("USG_PCT")),
                    pace=_f(row.get("PACE")),
                    pie=_f(row.get("PIE")),
                )
                validated.append(model)
            except (pydantic.ValidationError, ValueError) as exc:
                self.logger.warning(
                    "Advanced box score row validation failed",
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
                a = cast("BoxScoreAdvancedRowCreate", item)
                game_id = game_id or a.game_id
                if not require_fk(conn, "game", "game_id", a.game_id):
                    continue
                conn.execute(
                    """
                    INSERT INTO player_game_log_advanced
                        (game_id, player_id, team_id, minutes_played,
                         off_rating, def_rating, net_rating,
                         ast_pct, ast_to_tov, ast_ratio,
                         oreb_pct, dreb_pct, reb_pct, tov_pct,
                         efg_pct, ts_pct, usg_pct, pace, pie)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(game_id, player_id) DO UPDATE SET
                        minutes_played = excluded.minutes_played,
                        off_rating     = excluded.off_rating,
                        def_rating     = excluded.def_rating,
                        net_rating     = excluded.net_rating,
                        ast_pct        = excluded.ast_pct,
                        oreb_pct       = excluded.oreb_pct,
                        dreb_pct       = excluded.dreb_pct,
                        reb_pct        = excluded.reb_pct,
                        ts_pct         = excluded.ts_pct,
                        usg_pct        = excluded.usg_pct,
                        pace           = excluded.pace,
                        pie            = excluded.pie
                    """,
                    (
                        a.game_id,
                        a.player_id,
                        a.team_id,
                        a.minutes_played,
                        a.off_rating,
                        a.def_rating,
                        a.net_rating,
                        a.ast_pct,
                        a.ast_to_tov,
                        a.ast_ratio,
                        a.oreb_pct,
                        a.dreb_pct,
                        a.reb_pct,
                        a.tov_pct,
                        a.efg_pct,
                        a.ts_pct,
                        a.usg_pct,
                        a.pace,
                        a.pie,
                    ),
                )
                rows_affected += 1
            conn.execute("COMMIT")
            if game_id:
                set_game_availability_flag(conn, game_id, "box_score_advanced")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        upsert_audit(conn, self.entity_type, game_id or "all", "nba_api", "SUCCESS", rows_affected)
        self.logger.info("Upserted advanced box score rows", rows_affected=rows_affected)
        return rows_affected
