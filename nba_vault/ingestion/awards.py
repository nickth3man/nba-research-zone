"""Player awards ingestor.

Source: nba_api PlayerAwards endpoint.
Populates `award` table.
Available for all seasons (historical awards back to 1946).

The PlayerAwards endpoint returns: PERSON_ID, FIRST_NAME, LAST_NAME,
TEAM, DESCRIPTION, ALL_NBA_TEAM_NUMBER, SEASON, MONTH, WEEK, CONFERENCE,
TYPE, SUBTYPE1, SUBTYPE2, SUBTYPE3.

Mapping strategy:
  DESCRIPTION  → award_type  (normalised by AwardCreate validator)
  SUBTYPE1     → award_tier  (e.g. '1st Team', '2nd Team')
  SEASON       → season_id   (first 4 chars)
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
from nba_vault.models.entities import AwardCreate

logger = structlog.get_logger(__name__)

_AWARDS_DATASET = "PlayerAwards"


@register_ingestor
class AwardsIngestor(BaseIngestor):
    """
    Ingestor for individual player awards (MVP, All-Star, All-NBA, etc.).

    entity_id convention: "<player_id>" (NBA.com integer ID)

    Usage:
        ingestor = AwardsIngestor()
        result = ingestor.ingest("2544", conn)
    """

    entity_type = "awards"

    def __init__(self, cache: Any = None, rate_limiter: Any = None) -> None:
        super().__init__(cache, rate_limiter)
        self._client = NBAStatsClient(cache=self.cache, rate_limiter=self.rate_limiter)  # type: ignore[arg-type]

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        player_id = entity_id

        cache_key = f"awards_{player_id}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached  # type: ignore[return-value]

        self.rate_limiter.acquire()
        self.logger.info("Fetching awards", player_id=player_id)
        raw = self._client.adapter.get_player_awards(player_id=int(player_id))

        ds = raw.get(_AWARDS_DATASET, {})
        hdrs: list[str] = ds.get("headers", [])
        data: list[list[Any]] = ds.get("data", [])
        awards = [dict(zip(hdrs, row, strict=False)) for row in data]

        payload: dict[str, Any] = {
            "awards": awards,
            "player_id": player_id,
        }
        self.cache.set(cache_key, payload)
        self.logger.info("Fetched awards", player_id=player_id, count=len(awards))
        return payload

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        player_id = raw.get("player_id", "")
        validated: list[pydantic.BaseModel] = []
        for row in raw.get("awards", []):
            try:
                award_type_raw = str(row.get("DESCRIPTION", "") or "").strip()
                if not award_type_raw:
                    continue
                # Parse season_id from "YYYY-YY" format
                season_str = str(row.get("SEASON", "") or "").strip()
                season_year = _safe_int(season_str[:4]) if len(season_str) >= 4 else None
                if season_year is None:
                    continue
                # award_tier comes from SUBTYPE1 (e.g. "1st Team", "2nd Team")
                award_tier = str(row.get("SUBTYPE1", "") or "").strip() or None
                conference = str(row.get("CONFERENCE", "") or "").strip() or None
                model = AwardCreate(
                    player_id=int(player_id),
                    season_id=season_year,
                    award_type=award_type_raw,
                    award_tier=award_tier,
                    conference=conference,
                )
                validated.append(model)
            except (pydantic.ValidationError, ValueError) as exc:
                self.logger.warning("Award validation error", error=str(exc), row=row)
        self.logger.info("Validated awards", count=len(validated))
        return validated

    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        rows_affected = 0
        player_id = ""
        conn.execute("BEGIN")
        try:
            for item in model:
                a = cast("AwardCreate", item)
                player_id = str(a.player_id)
                if not require_fk(conn, "player", "player_id", a.player_id):
                    continue
                conn.execute(
                    """
                    INSERT INTO award
                        (player_id, season_id, award_type, award_tier, conference)
                    VALUES (?,?,?,?,?)
                    ON CONFLICT(player_id, season_id, award_type) DO UPDATE SET
                        award_tier  = excluded.award_tier,
                        conference  = excluded.conference
                    """,
                    (
                        a.player_id,
                        a.season_id,
                        a.award_type,
                        a.award_tier,
                        a.conference,
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
        self.logger.info("Upserted awards", rows_affected=rows_affected)
        return rows_affected


def _safe_int(val: Any) -> int | None:
    if val is None or str(val).strip() in ("", "None", "null"):
        return None
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None
