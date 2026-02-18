"""Draft history ingestor.

Source: nba_api DraftHistory endpoint.
Populates `draft_pick` table (maps to draft table in schema).
Available for all seasons (NBA/ABA/BAA 1947+).

Note: The DraftHistory endpoint returns ALL draft years at once.
entity_id is used for cache keying / filtering, not as an API parameter.
"""

from __future__ import annotations

import sqlite3
from typing import Any, cast

import pydantic
import structlog

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.nba_stats_client import NBAStatsClient
from nba_vault.ingestion.registry import register_ingestor
from nba_vault.ingestion.validation import upsert_audit
from nba_vault.models.entities import DraftPickCreate

logger = structlog.get_logger(__name__)

_DRAFT_DATASET = "DraftHistory"


@register_ingestor
class DraftIngestor(BaseIngestor):
    """
    Ingestor for draft pick history.

    entity_id convention: "<draft_year>" e.g. "2023"
      — used only for cache key; the API returns all years, then we filter.
      Pass "all" to load the entire history.

    Usage:
        ingestor = DraftIngestor()
        result = ingestor.ingest("2023", conn)   # load 2023 draft
        result = ingestor.ingest("all", conn)    # load entire history
    """

    entity_type = "draft"

    def __init__(self, cache: Any = None, rate_limiter: Any = None) -> None:
        super().__init__(cache, rate_limiter)
        self._client = NBAStatsClient(cache=self.cache, rate_limiter=self.rate_limiter)  # type: ignore[arg-type]

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        # entity_id is "all" or a 4-digit year string
        filter_year = None if entity_id == "all" else int(entity_id)

        cache_key = f"draft_{entity_id}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached  # type: ignore[return-value]

        self.rate_limiter.acquire()
        self.logger.info("Fetching draft history", filter_year=filter_year)
        # The adapter's get_draft_history() returns all years
        raw = self._client.adapter.get_draft_history()

        ds = raw.get(_DRAFT_DATASET, {})
        hdrs: list[str] = ds.get("headers", [])
        data: list[list[Any]] = ds.get("data", [])
        picks = [dict(zip(hdrs, row, strict=False)) for row in data]

        # Filter to requested year if specified
        if filter_year is not None:
            picks = [p for p in picks if int(p.get("SEASON", 0) or 0) == filter_year]

        payload: dict[str, Any] = {
            "picks": picks,
            "filter_year": filter_year,
        }
        self.cache.set(cache_key, payload)
        self.logger.info("Fetched draft picks", filter_year=filter_year, count=len(picks))
        return payload

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        validated: list[pydantic.BaseModel] = []
        for row in raw.get("picks", []):
            try:
                draft_year = _safe_int(row.get("SEASON")) or _safe_int(row.get("SEASON_YEAR"))
                if draft_year is None or draft_year < 1947:
                    continue
                draft_round = int(row.get("ROUND_NUMBER", 1) or 1)
                draft_number = int(row.get("OVERALL_PICK", 0) or 0)
                if draft_number == 0:
                    # Fall back to computing from round + pick
                    draft_number = int(row.get("ROUND_PICK", 0) or 0)
                if draft_number == 0:
                    continue
                model = DraftPickCreate(
                    draft_year=draft_year,
                    draft_round=draft_round,
                    draft_number=draft_number,
                    player_id=_safe_int(row.get("PERSON_ID")),
                    team_id=_safe_int(row.get("TEAM_ID")),
                    organization=str(row.get("ORGANIZATION", "") or "").strip() or None,
                    organization_type=str(row.get("ORGANIZATION_TYPE", "") or "").strip() or None,
                )
                validated.append(model)
            except (pydantic.ValidationError, ValueError) as exc:
                self.logger.warning("Draft pick validation error", error=str(exc), row=row)
        self.logger.info("Validated draft picks", count=len(validated))
        return validated

    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        rows_affected = 0
        label = ""
        conn.execute("BEGIN")
        try:
            for item in model:
                p = cast("DraftPickCreate", item)
                label = str(p.draft_year)
                conn.execute(
                    """
                    INSERT INTO draft
                        (draft_year, draft_round, draft_number,
                         player_id, team_id,
                         organization, organization_type)
                    VALUES (?,?,?,?,?,?,?)
                    ON CONFLICT(draft_year, draft_number) DO UPDATE SET
                        player_id         = excluded.player_id,
                        team_id           = excluded.team_id,
                        organization      = excluded.organization,
                        organization_type = excluded.organization_type
                    """,
                    (
                        p.draft_year,
                        p.draft_round,
                        p.draft_number,
                        p.player_id,
                        p.team_id,
                        p.organization,
                        p.organization_type,
                    ),
                )
                rows_affected += 1
            conn.execute("COMMIT")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        upsert_audit(conn, self.entity_type, label or "all", "nba_api", "SUCCESS", rows_affected)
        self.logger.info("Upserted draft picks", rows_affected=rows_affected)
        return rows_affected


def _safe_int(val: Any) -> int | None:
    if val is None or str(val).strip() in ("", "None", "null"):
        return None
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None
