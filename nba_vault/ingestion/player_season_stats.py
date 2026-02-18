"""Player season statistics ingestor.

Source: nba_api PlayerCareerStats endpoint.
Populates `player_season_stats` table (per-season per-game averages).
Available for all seasons (1946-present).
"""

from __future__ import annotations

import sqlite3
from typing import Any, cast

import pydantic
import structlog

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.nba_stats_client import NBAStatsClient
from nba_vault.ingestion.registry import register_ingestor
from nba_vault.ingestion.validation import require_fk, upsert_audit
from nba_vault.models.entities import PlayerSeasonStatsCreate

logger = structlog.get_logger(__name__)

# Dataset keys returned by PlayerCareerStats
_REG_TOTALS = "SeasonTotalsRegularSeason"


@register_ingestor
class PlayerSeasonStatsIngestor(BaseIngestor):
    """
    Ingestor for player career / per-season statistics.

    entity_id convention: "<player_id>" (NBA.com integer ID)
    kwargs:
        per_mode (str): "PerGame" (default), "Totals", "Per36"

    Usage:
        ingestor = PlayerSeasonStatsIngestor()
        result = ingestor.ingest("2544", conn)
    """

    entity_type = "player_season_stats"

    def __init__(self, cache: Any = None, rate_limiter: Any = None) -> None:
        super().__init__(cache, rate_limiter)
        self._client = NBAStatsClient(cache=self.cache, rate_limiter=self.rate_limiter)  # type: ignore[arg-type]

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        player_id = entity_id
        per_mode: str = str(kwargs.get("per_mode", "PerGame"))

        cache_key = f"player_career_{player_id}_{per_mode}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached  # type: ignore[return-value]

        self.rate_limiter.acquire()
        self.logger.info("Fetching player career stats", player_id=player_id)
        raw = self._client.adapter.get_player_career_stats(
            player_id=int(player_id),
            per_mode=per_mode,
        )

        def _parse_ds(key: str) -> list[dict[str, Any]]:
            ds = raw.get(key, {})
            hdrs: list[str] = ds.get("headers", [])
            data: list[list[Any]] = ds.get("data", [])
            return [dict(zip(hdrs, row, strict=False)) for row in data]

        payload: dict[str, Any] = {
            "totals": _parse_ds(_REG_TOTALS),
            "player_id": player_id,
        }
        self.cache.set(cache_key, payload)
        self.logger.info(
            "Fetched career stats",
            player_id=player_id,
            seasons=len(payload["totals"]),
        )
        return payload

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        player_id = raw.get("player_id", "")
        validated: list[pydantic.BaseModel] = []
        for row in raw.get("totals", []):
            try:
                season_str = str(row.get("SEASON_ID", "") or "")
                # SEASON_ID from NBA API is like "2023-24"
                season_year = _safe_int(season_str[:4]) if len(season_str) >= 4 else None
                if season_year is None:
                    continue
                team_id = _safe_int(row.get("TEAM_ID")) or 0
                model = PlayerSeasonStatsCreate(
                    player_id=int(player_id),
                    season_id=season_year,
                    team_id=team_id,
                    stat_type="Regular Season",
                    games_played=_safe_int(row.get("GP")),
                    games_started=_safe_int(row.get("GS")),
                    minutes_played=_safe_float(row.get("MIN")),
                    fgm=_safe_float(row.get("FGM")),
                    fga=_safe_float(row.get("FGA")),
                    fg_pct=_safe_float(row.get("FG_PCT")),
                    fg3m=_safe_float(row.get("FG3M")),
                    fg3a=_safe_float(row.get("FG3A")),
                    fg3_pct=_safe_float(row.get("FG3_PCT")),
                    ftm=_safe_float(row.get("FTM")),
                    fta=_safe_float(row.get("FTA")),
                    ft_pct=_safe_float(row.get("FT_PCT")),
                    oreb=_safe_float(row.get("OREB")),
                    dreb=_safe_float(row.get("DREB")),
                    reb=_safe_float(row.get("REB")),
                    ast=_safe_float(row.get("AST")),
                    stl=_safe_float(row.get("STL")),
                    blk=_safe_float(row.get("BLK")),
                    tov=_safe_float(row.get("TOV")),
                    pf=_safe_float(row.get("PF")),
                    pts=_safe_float(row.get("PTS")),
                )
                validated.append(model)
            except (pydantic.ValidationError, ValueError) as exc:
                self.logger.warning("Season stats validation error", error=str(exc), row=row)
        self.logger.info("Validated season stat rows", count=len(validated))
        return validated

    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        rows_affected = 0
        player_id = ""
        conn.execute("BEGIN")
        try:
            for item in model:
                s = cast("PlayerSeasonStatsCreate", item)
                player_id = str(s.player_id)
                if not require_fk(conn, "player", "player_id", s.player_id):
                    continue
                conn.execute(
                    """
                    INSERT INTO player_season_stats
                        (player_id, season_id, team_id, stat_type,
                         games_played, games_started, minutes_played,
                         fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
                         ftm, fta, ft_pct,
                         oreb, dreb, reb, ast, stl, blk, tov, pf, pts)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(player_id, season_id, team_id, stat_type) DO UPDATE SET
                        games_played   = excluded.games_played,
                        games_started  = excluded.games_started,
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
                        pts            = excluded.pts
                    """,
                    (
                        s.player_id,
                        s.season_id,
                        s.team_id,
                        s.stat_type,
                        s.games_played,
                        s.games_started,
                        s.minutes_played,
                        s.fgm,
                        s.fga,
                        s.fg_pct,
                        s.fg3m,
                        s.fg3a,
                        s.fg3_pct,
                        s.ftm,
                        s.fta,
                        s.ft_pct,
                        s.oreb,
                        s.dreb,
                        s.reb,
                        s.ast,
                        s.stl,
                        s.blk,
                        s.tov,
                        s.pf,
                        s.pts,
                    ),
                )
                rows_affected += 1
            conn.execute("COMMIT")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        upsert_audit(
            conn, self.entity_type, player_id or "all", "nba_api", "SUCCESS", rows_affected
        )
        self.logger.info("Upserted season stat rows", rows_affected=rows_affected)
        return rows_affected


def _safe_int(val: Any) -> int | None:
    if val is None or str(val).strip() in ("", "None", "null"):
        return None
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None


def _safe_float(val: Any) -> float | None:
    if val is None or str(val).strip() in ("", "None", "null"):
        return None
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None
