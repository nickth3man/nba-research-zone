"""Coach and roster ingestor.

Source: nba_api CommonTeamRoster endpoint.
Populates `coach` and `coach_stint` tables.
Available for all seasons.
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
from nba_vault.models.entities import CoachCreate, CoachStintCreate

logger = structlog.get_logger(__name__)

_COACHES_DATASET = "Coaches"


@register_ingestor
class CoachIngestor(BaseIngestor):
    """
    Ingestor for coaching staff data.

    entity_id convention: "<team_id>" (NBA.com integer ID)
    kwargs:
        season (str): Season string e.g. "2023-24"
        season_year (int): Integer start year of season e.g. 2023

    Usage:
        ingestor = CoachIngestor()
        result = ingestor.ingest("1610612747", conn, season="2023-24", season_year=2023)
    """

    entity_type = "coaches"

    def __init__(self, cache: Any = None, rate_limiter: Any = None) -> None:
        super().__init__(cache, rate_limiter)
        self._client = NBAStatsClient(cache=self.cache, rate_limiter=self.rate_limiter)  # type: ignore[arg-type]

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        team_id = entity_id
        season: str = str(kwargs.get("season", "2023-24"))
        season_year: int = int(kwargs.get("season_year", int(season.split("-", maxsplit=1)[0])))

        cache_key = f"coaches_{team_id}_{season}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached  # type: ignore[return-value]

        self.rate_limiter.acquire()
        self.logger.info("Fetching coaches", team_id=team_id, season=season)
        raw = self._client.adapter.get_common_team_roster(
            team_id=int(team_id),
            season=season,
        )

        ds = raw.get(_COACHES_DATASET, {})
        hdrs: list[str] = ds.get("headers", [])
        data: list[list[Any]] = ds.get("data", [])
        coaches = [dict(zip(hdrs, row, strict=False)) for row in data]

        payload: dict[str, Any] = {
            "coaches": coaches,
            "team_id": team_id,
            "season": season,
            "season_year": season_year,
        }
        self.cache.set(cache_key, payload)
        self.logger.info("Fetched coaches", team_id=team_id, count=len(coaches))
        return payload

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        team_id = raw.get("team_id", "")
        season_year = int(raw.get("season_year", 0))
        validated: list[pydantic.BaseModel] = []
        for row in raw.get("coaches", []):
            try:
                coach_id_raw = row.get("COACH_ID") or row.get("PERSON_ID") or 0
                coach_id = int(coach_id_raw or 0)
                if coach_id == 0:
                    continue
                first_name = str(row.get("FIRST_NAME", "") or "").strip()
                last_name = str(row.get("LAST_NAME", "") or "").strip()
                full_name = (
                    f"{first_name} {last_name}".strip()
                    or str(row.get("COACH_NAME", "") or "").strip()
                    or f"Coach {coach_id}"
                )
                coach_type_raw = str(row.get("COACH_TYPE", "") or "").strip()
                coach_type = coach_type_raw or "Assistant"
                coach = CoachCreate(
                    coach_id=coach_id,
                    first_name=first_name or "Unknown",
                    last_name=last_name or "Unknown",
                    full_name=full_name,
                    is_active=1,
                )
                stint = CoachStintCreate(
                    coach_id=coach_id,
                    team_id=int(team_id or 0),
                    season_id=season_year,
                    coach_type=coach_type,
                    sort_sequence=_safe_int(row.get("SORT_SEQUENCE")),
                )
                validated.append(coach)
                validated.append(stint)
            except (pydantic.ValidationError, ValueError) as exc:
                self.logger.warning("Coach validation error", error=str(exc), row=row)
        self.logger.info("Validated coaches", count=len(validated) // 2)
        return validated

    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        rows_affected = 0
        entity_id = ""
        conn.execute("BEGIN")
        try:
            for item in model:
                if isinstance(item, CoachCreate):
                    c = cast("CoachCreate", item)
                    entity_id = str(c.coach_id)
                    conn.execute(
                        """
                        INSERT INTO coach
                            (coach_id, first_name, last_name, full_name, is_active)
                        VALUES (?,?,?,?,?)
                        ON CONFLICT(coach_id) DO UPDATE SET
                            full_name  = excluded.full_name,
                            first_name = excluded.first_name,
                            last_name  = excluded.last_name,
                            is_active  = excluded.is_active
                        """,
                        (c.coach_id, c.first_name, c.last_name, c.full_name, c.is_active),
                    )
                    rows_affected += 1
                elif isinstance(item, CoachStintCreate):
                    s = cast("CoachStintCreate", item)
                    if not require_fk(conn, "coach", "coach_id", s.coach_id):
                        continue
                    conn.execute(
                        """
                        INSERT INTO coach_stint
                            (coach_id, team_id, season_id, coach_type, sort_sequence)
                        VALUES (?,?,?,?,?)
                        ON CONFLICT(coach_id, team_id, season_id) DO UPDATE SET
                            coach_type    = excluded.coach_type,
                            sort_sequence = excluded.sort_sequence
                        """,
                        (
                            s.coach_id,
                            s.team_id,
                            s.season_id,
                            s.coach_type,
                            s.sort_sequence,
                        ),
                    )
                    rows_affected += 1
            conn.execute("COMMIT")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        upsert_audit(
            conn, self.entity_type, entity_id or "all", "nba_api", "SUCCESS", rows_affected
        )
        self.logger.info("Upserted coaches", rows_affected=rows_affected)
        return rows_affected


def _safe_int(val: Any) -> int | None:
    if val is None or str(val).strip() in ("", "None", "null"):
        return None
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None
