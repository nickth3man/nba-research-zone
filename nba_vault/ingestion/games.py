"""Game schedule and official ingestors.

Sources:
- GameScheduleIngestor: nba_api LeagueGameLog endpoint — fetches all games for a
  given season/season_type, populating the `game` table.
- GameOfficialIngestor: nba_api BoxScoreSummaryV2 endpoint — extracts official
  assignments per game, populating `official` and `game_official` tables.
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
from nba_vault.models.entities import GameCreate, GameOfficialCreate, OfficialCreate

logger = structlog.get_logger(__name__)

# LeagueGameLog dataset name returned by the adapter
_GAME_LOG_DATASET = "LeagueGameLog"
# BoxScoreSummaryV2 dataset names
_OFFICIALS_DATASET = "Officials"


@register_ingestor
class GameScheduleIngestor(BaseIngestor):
    """
    Ingestor for NBA game schedule / results.

    Fetches all completed games for a season via LeagueGameLog and upserts
    them into the `game` table.  One call covers an entire season; call once
    per season_type if playoffs are also needed.

    entity_id convention: "<season>" e.g. "2023-24"
    kwargs:
        season_type (str): "Regular Season" | "Playoffs" | "Pre Season"

    Usage:
        ingestor = GameScheduleIngestor()
        result = ingestor.ingest("2023-24", conn)
        result = ingestor.ingest("2023-24", conn, season_type="Playoffs")
    """

    entity_type = "game_schedule"

    def __init__(self, cache: Any = None, rate_limiter: Any = None) -> None:
        super().__init__(cache, rate_limiter)
        self._client = NBAStatsClient(cache=self.cache, rate_limiter=self.rate_limiter)  # type: ignore[arg-type]

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        season = entity_id
        season_type: str = kwargs.get("season_type", "Regular Season")
        cache_key = f"game_schedule_{season}_{season_type}"
        cached = self.cache.get(cache_key)
        if cached:
            self.logger.debug("Cache hit for game schedule", season=season)
            return cached  # type: ignore[return-value]

        self.rate_limiter.acquire()
        self.logger.info("Fetching game schedule", season=season, season_type=season_type)
        raw = self._client.adapter.get_league_game_log(
            season=season,
            season_type=season_type,
        )
        dataset = raw.get(_GAME_LOG_DATASET, {})
        headers: list[str] = dataset.get("headers", [])
        rows: list[list[Any]] = dataset.get("data", [])
        games = [dict(zip(headers, row, strict=False)) for row in rows]
        payload: dict[str, Any] = {
            "games": games,
            "season": season,
            "season_type": season_type,
        }
        self.cache.set(cache_key, payload)
        self.logger.info("Fetched games", count=len(games), season=season)
        return payload

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        season_label: str = raw.get("season", "")
        season_year = int(season_label.split("-", maxsplit=1)[0]) if season_label else 0
        validated: list[pydantic.BaseModel] = []
        for row in raw.get("games", []):
            try:
                game_id = str(row.get("GAME_ID", "")).zfill(10)
                home_team_id = int(row.get("TEAM_ID", 0))
                # LeagueGameLog returns one row per team per game; deduplicate by
                # taking the row where MATCHUP contains "vs." (home-team row).
                matchup: str = str(row.get("MATCHUP", ""))
                if "vs." not in matchup:
                    continue
                wl: str = str(row.get("WL", ""))
                winner_team_id = home_team_id if wl == "W" else None
                model = GameCreate(
                    game_id=game_id,
                    season_id=season_year,
                    game_date=str(row.get("GAME_DATE", ""))[:10],
                    game_type="Regular Season",
                    home_team_id=home_team_id,
                    away_team_id=0,
                    winner_team_id=winner_team_id,
                    home_team_score=int(row.get("PTS", 0) or 0),
                    away_team_score=None,
                    attendance=None,
                    arena_id=None,
                )
                validated.append(model)
            except (pydantic.ValidationError, ValueError) as exc:
                self.logger.warning(
                    "Game validation failed",
                    game_id=row.get("GAME_ID"),
                    error=str(exc),
                )
        self.logger.info("Validated games", count=len(validated))
        return validated

    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        rows_affected = 0
        conn.execute("BEGIN")
        try:
            for item in model:
                g = cast("GameCreate", item)
                if not require_fk(conn, "season", "season_id", g.season_id):
                    self.logger.warning(
                        "Season FK missing, skipping game",
                        game_id=g.game_id,
                        season_id=g.season_id,
                    )
                    continue
                conn.execute(
                    """
                    INSERT INTO game
                        (game_id, season_id, game_date, home_team_id, away_team_id,
                         winner_team_id, home_team_score, away_team_score,
                         attendance, arena_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(game_id) DO UPDATE SET
                        season_id       = excluded.season_id,
                        game_date       = excluded.game_date,
                        home_team_id    = excluded.home_team_id,
                        winner_team_id  = excluded.winner_team_id,
                        home_team_score = excluded.home_team_score,
                        away_team_score = excluded.away_team_score,
                        attendance      = excluded.attendance,
                        arena_id        = excluded.arena_id
                    """,
                    (
                        g.game_id,
                        g.season_id,
                        g.game_date,
                        g.home_team_id,
                        g.away_team_id,
                        g.winner_team_id,
                        g.home_team_score,
                        g.away_team_score,
                        g.attendance,
                        g.arena_id,
                    ),
                )
                rows_affected += 1
            conn.execute("COMMIT")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        upsert_audit(conn, self.entity_type, "all", "nba_api", "SUCCESS", rows_affected)
        self.logger.info("Upserted games", rows_affected=rows_affected)
        return rows_affected


@register_ingestor
class GameOfficialIngestor(BaseIngestor):
    """
    Ingestor for game officials (referees).

    Fetches BoxScoreSummaryV2 for a given game_id and upserts officials into
    the `official` and `game_official` tables.

    entity_id convention: "<game_id>" e.g. "0022300001"

    Usage:
        ingestor = GameOfficialIngestor()
        result = ingestor.ingest("0022300001", conn)
    """

    entity_type = "game_officials"

    def __init__(self, cache: Any = None, rate_limiter: Any = None) -> None:
        super().__init__(cache, rate_limiter)
        self._client = NBAStatsClient(cache=self.cache, rate_limiter=self.rate_limiter)  # type: ignore[arg-type]

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        game_id = entity_id
        cache_key = f"box_score_summary_{game_id}"
        cached = self.cache.get(cache_key)
        if cached:
            self.logger.debug("Cache hit for box score summary", game_id=game_id)
            return cached  # type: ignore[return-value]

        self.rate_limiter.acquire()
        self.logger.info("Fetching box score summary for officials", game_id=game_id)
        raw = self._client.adapter.get_box_score_summary(game_id=game_id)

        ds = raw.get(_OFFICIALS_DATASET, {})
        headers: list[str] = ds.get("headers", [])
        rows: list[list[Any]] = ds.get("data", [])
        officials = [dict(zip(headers, row, strict=False)) for row in rows]

        payload: dict[str, Any] = {"officials": officials, "game_id": game_id}
        self.cache.set(cache_key, payload)
        self.logger.info("Fetched officials", count=len(officials), game_id=game_id)
        return payload

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        game_id = raw.get("game_id", "")
        validated: list[pydantic.BaseModel] = []
        for row in raw.get("officials", []):
            try:
                first = str(row.get("FIRST_NAME", ""))
                last = str(row.get("LAST_NAME", ""))
                official = OfficialCreate(
                    official_id=int(row.get("OFFICIAL_ID", 0)),
                    first_name=first,
                    last_name=last,
                    full_name=f"{first} {last}".strip(),
                    jersey_num=str(row.get("JERSEY_NUM", "") or ""),
                )
                game_official = GameOfficialCreate(
                    game_id=game_id,
                    official_id=int(row.get("OFFICIAL_ID", 0)),
                    assignment=str(row.get("ASSIGNMENT", "") or ""),
                )
                validated.append(official)
                validated.append(game_official)
            except (pydantic.ValidationError, ValueError) as exc:
                self.logger.warning(
                    "Official validation failed",
                    game_id=game_id,
                    row=row,
                    error=str(exc),
                )
        return validated

    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        rows_affected = 0
        conn.execute("BEGIN")
        try:
            for item in model:
                if isinstance(item, OfficialCreate):
                    o = cast("OfficialCreate", item)
                    conn.execute(
                        """
                        INSERT INTO official
                            (official_id, first_name, last_name, full_name, jersey_num)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(official_id) DO UPDATE SET
                            first_name = excluded.first_name,
                            last_name  = excluded.last_name,
                            full_name  = excluded.full_name,
                            jersey_num = excluded.jersey_num
                        """,
                        (o.official_id, o.first_name, o.last_name, o.full_name, o.jersey_num),
                    )
                    rows_affected += 1
                elif isinstance(item, GameOfficialCreate):
                    go = cast("GameOfficialCreate", item)
                    if not require_fk(conn, "game", "game_id", go.game_id):
                        self.logger.warning(
                            "Game FK missing for game_official, skipping",
                            game_id=go.game_id,
                        )
                        continue
                    conn.execute(
                        """
                        INSERT INTO game_official
                            (game_id, official_id, assignment)
                        VALUES (?, ?, ?)
                        ON CONFLICT(game_id, official_id) DO UPDATE SET
                            assignment = excluded.assignment
                        """,
                        (go.game_id, go.official_id, go.assignment),
                    )
                    rows_affected += 1
            conn.execute("COMMIT")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        upsert_audit(conn, self.entity_type, "all", "nba_api", "SUCCESS", rows_affected)
        self.logger.info("Upserted game officials", rows_affected=rows_affected)
        return rows_affected
