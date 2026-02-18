"""Play-by-play event ingestor.

Source: nba_api PlayByPlayV2 endpoint.
Populates `play_by_play` table.
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
from nba_vault.models.entities import PlayByPlayEventCreate

logger = structlog.get_logger(__name__)

_PBP_DATASET = "PlayByPlay"


@register_ingestor
class PlayByPlayIngestor(BaseIngestor):
    """
    Ingestor for play-by-play events (1996-97+).

    entity_id convention: "<game_id>" e.g. "0022300001"
    kwargs:
        season_year (int): Integer start year of season e.g. 2023

    Usage:
        ingestor = PlayByPlayIngestor()
        result = ingestor.ingest("0022300001", conn, season_year=2023)
    """

    entity_type = "play_by_play"

    def __init__(self, cache: Any = None, rate_limiter: Any = None) -> None:
        super().__init__(cache, rate_limiter)
        self._client = NBAStatsClient(cache=self.cache, rate_limiter=self.rate_limiter)  # type: ignore[arg-type]

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        game_id = entity_id
        season_year: int = int(kwargs.get("season_year", 0))
        check_data_availability("play_by_play", season_year)

        cache_key = f"play_by_play_{game_id}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached  # type: ignore[return-value]

        self.rate_limiter.acquire()
        self.logger.info("Fetching play-by-play", game_id=game_id)
        raw = self._client.adapter.get_play_by_play(game_id=game_id)

        ds = raw.get(_PBP_DATASET, {})
        hdrs: list[str] = ds.get("headers", [])
        data: list[list[Any]] = ds.get("data", [])
        events = [dict(zip(hdrs, row, strict=False)) for row in data]

        payload: dict[str, Any] = {
            "events": events,
            "game_id": game_id,
            "season_year": season_year,
        }
        self.cache.set(cache_key, payload)
        self.logger.info("Fetched PBP events", game_id=game_id, count=len(events))
        return payload

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        game_id = raw.get("game_id", "")
        validated: list[pydantic.BaseModel] = []
        for row in raw.get("events", []):
            try:
                event_type = int(row.get("EVENTMSGTYPE", 0) or 0)
                if event_type < 1 or event_type > 21:
                    # Skip unknown event types (e.g. 0 = unknown/technical)
                    continue
                model = PlayByPlayEventCreate(
                    game_id=game_id,
                    event_num=int(row.get("EVENTNUM", 0) or 0),
                    period=int(row.get("PERIOD", 1) or 1),
                    pc_time=int(
                        row.get("PCTIMESTRING", "0").replace(":", "").replace("-", "0") or 0
                    )
                    if row.get("PCTIMESTRING")
                    else None,
                    wc_time=str(row.get("WCTIMESTRING", "") or "") or None,
                    event_type=event_type,
                    event_action_type=int(row.get("EVENTMSGACTIONTYPE", 0) or 0),
                    description_home=str(row.get("HOMEDESCRIPTION", "") or "") or None,
                    description_visitor=str(row.get("VISITORDESCRIPTION", "") or "") or None,
                    score_home=_safe_int(row.get("SCORE", "").split("-")[0])
                    if row.get("SCORE")
                    else None,
                    score_visitor=_safe_int(row.get("SCORE", "").split("-")[1])
                    if row.get("SCORE") and "-" in str(row.get("SCORE", ""))
                    else None,
                    score_margin=_safe_int(row.get("SCOREMARGIN")),
                    player1_id=_safe_int(row.get("PLAYER1_ID")),
                    player1_team_id=_safe_int(row.get("PLAYER1_TEAM_ID")),
                    player2_id=_safe_int(row.get("PLAYER2_ID")),
                    player2_team_id=_safe_int(row.get("PLAYER2_TEAM_ID")),
                    player3_id=_safe_int(row.get("PLAYER3_ID")),
                    player3_team_id=_safe_int(row.get("PLAYER3_TEAM_ID")),
                    video_available=int(row.get("VIDEO_AVAILABLE_FLAG", 0) or 0),
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
        self.logger.info("Validated PBP events", count=len(validated))
        return validated

    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        game_id: str = ""
        rows_affected = 0
        conn.execute("BEGIN")
        try:
            for item in model:
                e = cast("PlayByPlayEventCreate", item)
                game_id = game_id or e.game_id
                if not require_fk(conn, "game", "game_id", e.game_id):
                    continue
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
                        description_home    = excluded.description_home,
                        description_visitor = excluded.description_visitor,
                        score_home          = excluded.score_home,
                        score_visitor       = excluded.score_visitor,
                        score_margin        = excluded.score_margin,
                        video_available     = excluded.video_available
                    """,
                    (
                        e.game_id,
                        e.event_num,
                        e.period,
                        e.pc_time,
                        e.wc_time,
                        e.event_type,
                        e.event_action_type,
                        e.description_home,
                        e.description_visitor,
                        e.score_home,
                        e.score_visitor,
                        e.score_margin,
                        e.player1_id,
                        e.player1_team_id,
                        e.player2_id,
                        e.player2_team_id,
                        e.player3_id,
                        e.player3_team_id,
                        e.video_available,
                    ),
                )
                rows_affected += 1
            conn.execute("COMMIT")
            if game_id:
                set_game_availability_flag(conn, game_id, "play_by_play")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        upsert_audit(conn, self.entity_type, game_id or "all", "nba_api", "SUCCESS", rows_affected)
        self.logger.info("Upserted PBP events", rows_affected=rows_affected)
        return rows_affected


def _safe_int(val: Any) -> int | None:
    if val is None or str(val).strip() in ("", "None", "null"):
        return None
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None
