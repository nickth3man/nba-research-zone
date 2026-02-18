"""Traditional box score ingestor.

Source: nba_api BoxScoreTraditionalV2 endpoint.
Populates `player_game_log` and `team_game_log` tables.
Available for all NBA.com era games (approx. 1996-97+).
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
from nba_vault.models.entities import BoxScorePlayerRowCreate, BoxScoreTeamRowCreate

logger = structlog.get_logger(__name__)

_PLAYER_STATS_DATASET = "PlayerStats"
_TEAM_STATS_DATASET = "TeamStats"


def _i(val: Any, default: int = 0) -> int | None:
    """Safe int coercion; returns None if val is None/empty."""
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return default if default is not None else None


def _f(val: Any, default: float = 0.0) -> float | None:
    """Safe float coercion; returns None if val is None/empty."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


@register_ingestor
class BoxScoreTraditionalIngestor(BaseIngestor):
    """
    Ingestor for traditional per-game box scores (player + team rows).

    entity_id convention: "<game_id>" e.g. "0022300001"
    kwargs:
        season_year (int): Integer year of season start, used for era gate.
                           e.g. 2023 for 2023-24

    Usage:
        ingestor = BoxScoreTraditionalIngestor()
        result = ingestor.ingest("0022300001", conn, season_year=2023)
    """

    entity_type = "box_score_traditional"

    def __init__(self, cache: Any = None, rate_limiter: Any = None) -> None:
        super().__init__(cache, rate_limiter)
        self._client = NBAStatsClient(cache=self.cache, rate_limiter=self.rate_limiter)  # type: ignore[arg-type]

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        game_id = entity_id
        season_year: int = int(kwargs.get("season_year", 0))
        check_data_availability("box_score_traditional", season_year)

        cache_key = f"box_score_traditional_{game_id}"
        cached = self.cache.get(cache_key)
        if cached:
            self.logger.debug("Cache hit for traditional box score", game_id=game_id)
            return cached  # type: ignore[return-value]

        self.rate_limiter.acquire()
        self.logger.info("Fetching traditional box score", game_id=game_id)
        raw = self._client.adapter.get_box_score_traditional(game_id=game_id)

        def _extract(key: str) -> list[dict[str, Any]]:
            ds = raw.get(key, {})
            hdrs: list[str] = ds.get("headers", [])
            data: list[list[Any]] = ds.get("data", [])
            return [dict(zip(hdrs, row, strict=False)) for row in data]

        payload: dict[str, Any] = {
            "player_rows": _extract(_PLAYER_STATS_DATASET),
            "team_rows": _extract(_TEAM_STATS_DATASET),
            "game_id": game_id,
            "season_year": season_year,
        }
        self.cache.set(cache_key, payload)
        self.logger.info(
            "Fetched box score rows",
            game_id=game_id,
            player_rows=len(payload["player_rows"]),
            team_rows=len(payload["team_rows"]),
        )
        return payload

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        game_id = raw.get("game_id", "")
        season_year: int = int(raw.get("season_year", 0))
        validated: list[pydantic.BaseModel] = []

        for row in raw.get("player_rows", []):
            try:
                # MIN is "MM:SS" — convert to float minutes
                min_str: str = str(row.get("MIN", "") or "")
                mins: float | None = None
                if ":" in min_str:
                    parts = min_str.split(":")
                    mins = float(parts[0]) + float(parts[1]) / 60
                elif min_str:
                    mins = float(min_str)

                model = BoxScorePlayerRowCreate(
                    game_id=game_id,
                    player_id=int(row.get("PLAYER_ID", 0)),
                    team_id=int(row.get("TEAM_ID", 0)),
                    season_id=season_year,
                    start_position=str(row.get("START_POSITION", "") or "") or None,
                    comment=str(row.get("COMMENT", "") or "") or None,
                    minutes_played=mins,
                    fgm=_i(row.get("FGM")),
                    fga=_i(row.get("FGA")),
                    fg_pct=_f(row.get("FG_PCT")),
                    fg3m=_i(row.get("FG3M")),
                    fg3a=_i(row.get("FG3A")),
                    fg3_pct=_f(row.get("FG3_PCT")),
                    ftm=_i(row.get("FTM")),
                    fta=_i(row.get("FTA")),
                    ft_pct=_f(row.get("FT_PCT")),
                    oreb=_i(row.get("OREB")),
                    dreb=_i(row.get("DREB")),
                    reb=_i(row.get("REB")),
                    ast=_i(row.get("AST")),
                    stl=_i(row.get("STL")),
                    blk=_i(row.get("BLK")),
                    tov=_i(row.get("TO")),
                    pf=_i(row.get("PF")),
                    pts=_i(row.get("PTS")),
                    plus_minus=_i(row.get("PLUS_MINUS")),
                )
                validated.append(model)
            except (pydantic.ValidationError, ValueError) as exc:
                self.logger.warning(
                    "Player box score row validation failed",
                    game_id=game_id,
                    player_id=row.get("PLAYER_ID"),
                    error=str(exc),
                )

        for row in raw.get("team_rows", []):
            try:
                model = BoxScoreTeamRowCreate(
                    game_id=game_id,
                    team_id=int(row.get("TEAM_ID", 0)),
                    season_id=season_year,
                    is_home=0,  # determined at query time from game table
                    fgm=_i(row.get("FGM")),
                    fga=_i(row.get("FGA")),
                    fg_pct=_f(row.get("FG_PCT")),
                    fg3m=_i(row.get("FG3M")),
                    fg3a=_i(row.get("FG3A")),
                    fg3_pct=_f(row.get("FG3_PCT")),
                    ftm=_i(row.get("FTM")),
                    fta=_i(row.get("FTA")),
                    ft_pct=_f(row.get("FT_PCT")),
                    oreb=_i(row.get("OREB")),
                    dreb=_i(row.get("DREB")),
                    reb=_i(row.get("REB")),
                    ast=_i(row.get("AST")),
                    stl=_i(row.get("STL")),
                    blk=_i(row.get("BLK")),
                    tov=_i(row.get("TO")),
                    pf=_i(row.get("PF")),
                    pts=_i(row.get("PTS")),
                )
                validated.append(model)
            except (pydantic.ValidationError, ValueError) as exc:
                self.logger.warning(
                    "Team box score row validation failed",
                    game_id=game_id,
                    team_id=row.get("TEAM_ID"),
                    error=str(exc),
                )

        self.logger.info("Validated box score rows", count=len(validated))
        return validated

    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        game_id: str = ""
        rows_affected = 0
        conn.execute("BEGIN")
        try:
            for item in model:
                if isinstance(item, BoxScorePlayerRowCreate):
                    p = cast("BoxScorePlayerRowCreate", item)
                    game_id = game_id or p.game_id
                    if not require_fk(conn, "game", "game_id", p.game_id):
                        continue
                    conn.execute(
                        """
                        INSERT INTO player_game_log
                            (game_id, team_id, player_id, season_id,
                             start_position, comment, minutes_played,
                             fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
                             ftm, fta, ft_pct, oreb, dreb, reb,
                             ast, stl, blk, tov, pf, pts, plus_minus)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(game_id, player_id) DO UPDATE SET
                            start_position = excluded.start_position,
                            minutes_played = excluded.minutes_played,
                            fgm = excluded.fgm, fga = excluded.fga,
                            fg3m = excluded.fg3m, fg3a = excluded.fg3a,
                            ftm = excluded.ftm, fta = excluded.fta,
                            oreb = excluded.oreb, dreb = excluded.dreb,
                            reb = excluded.reb, ast = excluded.ast,
                            stl = excluded.stl, blk = excluded.blk,
                            tov = excluded.tov, pf = excluded.pf,
                            pts = excluded.pts, plus_minus = excluded.plus_minus
                        """,
                        (
                            p.game_id,
                            p.team_id,
                            p.player_id,
                            p.season_id,
                            p.start_position,
                            p.comment,
                            p.minutes_played,
                            p.fgm,
                            p.fga,
                            p.fg_pct,
                            p.fg3m,
                            p.fg3a,
                            p.fg3_pct,
                            p.ftm,
                            p.fta,
                            p.ft_pct,
                            p.oreb,
                            p.dreb,
                            p.reb,
                            p.ast,
                            p.stl,
                            p.blk,
                            p.tov,
                            p.pf,
                            p.pts,
                            p.plus_minus,
                        ),
                    )
                    rows_affected += 1

                elif isinstance(item, BoxScoreTeamRowCreate):
                    t = cast("BoxScoreTeamRowCreate", item)
                    game_id = game_id or t.game_id
                    if not require_fk(conn, "game", "game_id", t.game_id):
                        continue
                    conn.execute(
                        """
                        INSERT INTO team_game_log
                            (game_id, team_id, season_id, is_home,
                             fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
                             ftm, fta, ft_pct, oreb, dreb, reb,
                             ast, stl, blk, tov, pf, pts)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(game_id, team_id) DO UPDATE SET
                            fgm = excluded.fgm, fga = excluded.fga,
                            fg3m = excluded.fg3m, fg3a = excluded.fg3a,
                            ftm = excluded.ftm, fta = excluded.fta,
                            oreb = excluded.oreb, dreb = excluded.dreb,
                            reb = excluded.reb, ast = excluded.ast,
                            stl = excluded.stl, blk = excluded.blk,
                            tov = excluded.tov, pf = excluded.pf,
                            pts = excluded.pts
                        """,
                        (
                            t.game_id,
                            t.team_id,
                            t.season_id,
                            t.is_home,
                            t.fgm,
                            t.fga,
                            t.fg_pct,
                            t.fg3m,
                            t.fg3a,
                            t.fg3_pct,
                            t.ftm,
                            t.fta,
                            t.ft_pct,
                            t.oreb,
                            t.dreb,
                            t.reb,
                            t.ast,
                            t.stl,
                            t.blk,
                            t.tov,
                            t.pf,
                            t.pts,
                        ),
                    )
                    rows_affected += 1

            conn.execute("COMMIT")
            if game_id:
                set_game_availability_flag(conn, game_id, "box_score_traditional")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        upsert_audit(conn, self.entity_type, game_id or "all", "nba_api", "SUCCESS", rows_affected)
        self.logger.info("Upserted box score rows", rows_affected=rows_affected)
        return rows_affected
