"""Shot chart ingestor.

Source: nba_api ShotChartDetail endpoint.
Populates `shot_chart` table.
Era gate: available from 1996-97 season onwards.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, cast

import pydantic
import structlog

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.nba_stats_client import NBAStatsClient
from nba_vault.ingestion.registry import register_ingestor
from nba_vault.ingestion.validation import (
    check_data_availability,
    quarantine_row,
    require_fk,
    set_game_availability_flag,
    upsert_audit,
)
from nba_vault.models.entities import ShotChartRowCreate

logger = structlog.get_logger(__name__)

_SHOT_CHART_DATASET = "Shot_Chart_Detail"


@register_ingestor
class ShotChartIngestor(BaseIngestor):
    """
    Ingestor for shot chart data (1996-97+).

    Can be run per-game (pass game_id) or per-player/season.

    entity_id convention: "<game_id>" e.g. "0022300001"
    kwargs:
        season_year (int): Integer start year of season e.g. 2023
        player_id   (int): Optional player filter (0 = all players)
        team_id     (int): Optional team filter (0 = all teams)
        season      (str): Season string e.g. "2023-24"

    Usage:
        ingestor = ShotChartIngestor()
        result = ingestor.ingest("0022300001", conn, season_year=2023)
    """

    entity_type = "shot_chart"

    def __init__(self, cache: Any = None, rate_limiter: Any = None) -> None:
        super().__init__(cache, rate_limiter)
        self._client = NBAStatsClient(cache=self.cache, rate_limiter=self.rate_limiter)  # type: ignore[arg-type]

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        game_id = entity_id
        season_year: int = int(kwargs.get("season_year", 0))
        check_data_availability("shot_chart", season_year)

        player_id: int = int(kwargs.get("player_id", 0))
        team_id: int = int(kwargs.get("team_id", 0))
        season: str = str(kwargs.get("season", ""))

        cache_key = f"shot_chart_{game_id}_{player_id}_{team_id}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached  # type: ignore[return-value]

        self.rate_limiter.acquire()
        self.logger.info(
            "Fetching shot chart",
            game_id=game_id,
            player_id=player_id,
            team_id=team_id,
        )
        raw = self._client.adapter.get_shot_chart(
            game_id=game_id,
            player_id=player_id,
            team_id=team_id,
            season=season,
        )

        ds = raw.get(_SHOT_CHART_DATASET, {})
        hdrs: list[str] = ds.get("headers", [])
        data: list[list[Any]] = ds.get("data", [])
        shots = [dict(zip(hdrs, row, strict=False)) for row in data]

        payload: dict[str, Any] = {
            "shots": shots,
            "game_id": game_id,
            "season_year": season_year,
        }
        self.cache.set(cache_key, payload)
        self.logger.info("Fetched shots", game_id=game_id, count=len(shots))
        return payload

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        game_id = raw.get("game_id", "")
        validated: list[pydantic.BaseModel] = []
        for row in raw.get("shots", []):
            try:
                shot_gid = str(row.get("GAME_ID", game_id) or game_id).zfill(10)
                model = ShotChartRowCreate(
                    game_id=shot_gid,
                    player_id=int(row.get("PLAYER_ID", 0) or 0),
                    team_id=int(row.get("TEAM_ID", 0) or 0),
                    period=int(row.get("PERIOD", 1) or 1),
                    minutes_remaining=_safe_int(row.get("MINUTES_REMAINING")),
                    seconds_remaining=_safe_int(row.get("SECONDS_REMAINING")),
                    action_type=str(row.get("ACTION_TYPE", "") or "") or None,
                    shot_type=str(row.get("SHOT_TYPE", "") or "") or None,
                    shot_zone_basic=str(row.get("SHOT_ZONE_BASIC", "") or "") or None,
                    shot_zone_area=str(row.get("SHOT_ZONE_AREA", "") or "") or None,
                    shot_zone_range=str(row.get("SHOT_ZONE_RANGE", "") or "") or None,
                    shot_distance=_safe_int(row.get("SHOT_DISTANCE")),
                    loc_x=_safe_int(row.get("LOC_X")),
                    loc_y=_safe_int(row.get("LOC_Y")),
                    shot_made_flag=int(row.get("SHOT_MADE_FLAG", 0) or 0),
                    htm=str(row.get("HTM", "") or "") or None,
                    vtm=str(row.get("VTM", "") or "") or None,
                )
                validated.append(model)
            except (pydantic.ValidationError, ValueError) as exc:
                quarantine_row(
                    Path("data/quarantine"),
                    self.entity_type,
                    game_id,
                    row,
                    f"validation_error: {exc}",
                )
        self.logger.info("Validated shots", count=len(validated))
        return validated

    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        game_id: str = ""
        rows_affected = 0
        conn.execute("BEGIN")
        try:
            for item in model:
                s = cast("ShotChartRowCreate", item)
                game_id = game_id or s.game_id
                if not require_fk(conn, "game", "game_id", s.game_id):
                    continue
                conn.execute(
                    """
                    INSERT INTO shot_chart
                        (game_id, player_id, team_id, period,
                         minutes_remaining, seconds_remaining,
                         action_type, shot_type,
                         shot_zone_basic, shot_zone_area, shot_zone_range,
                         shot_distance, loc_x, loc_y, shot_made_flag,
                         htm, vtm)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(game_id, player_id, period, loc_x, loc_y) DO NOTHING
                    """,
                    (
                        s.game_id,
                        s.player_id,
                        s.team_id,
                        s.period,
                        s.minutes_remaining,
                        s.seconds_remaining,
                        s.action_type,
                        s.shot_type,
                        s.shot_zone_basic,
                        s.shot_zone_area,
                        s.shot_zone_range,
                        s.shot_distance,
                        s.loc_x,
                        s.loc_y,
                        s.shot_made_flag,
                        s.htm,
                        s.vtm,
                    ),
                )
                rows_affected += 1
            conn.execute("COMMIT")
            if game_id:
                set_game_availability_flag(conn, game_id, "shot_chart")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        upsert_audit(conn, self.entity_type, game_id or "all", "nba_api", "SUCCESS", rows_affected)
        self.logger.info("Upserted shots", rows_affected=rows_affected)
        return rows_affected


def _safe_int(val: Any) -> int | None:
    if val is None or str(val).strip() in ("", "None", "null"):
        return None
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None
