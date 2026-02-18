"""Player biographical enrichment ingestor.

Source: nba_api CommonPlayerInfo endpoint.
Enriches the `player` table with biographical fields: position, height_inches,
weight_lbs, birthdate, draft year/round/pick, country, college.
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
from nba_vault.models.entities import PlayerBioCreate

logger = structlog.get_logger(__name__)

_COMMON_PLAYER_INFO_DATASET = "CommonPlayerInfo"


def _parse_height_inches(height_str: str) -> float | None:
    """Parse '6-9' format into total inches (81.0)."""
    if not height_str:
        return None
    parts = height_str.split("-")
    if len(parts) == 2:
        try:
            return float(parts[0]) * 12 + float(parts[1])
        except ValueError:
            pass
    return None


@register_ingestor
class PlayerBioIngestor(BaseIngestor):
    """
    Ingestor for player biographical data via CommonPlayerInfo.

    Fetches biographical fields for a single player and enriches the `player`
    table (UPDATE only — the player row must already exist from PlayerIngestor).

    entity_id convention: "<player_id>" e.g. "2544"

    Usage:
        ingestor = PlayerBioIngestor()
        result = ingestor.ingest("2544", conn)
    """

    entity_type = "player_bio"

    def __init__(self, cache: Any = None, rate_limiter: Any = None) -> None:
        super().__init__(cache, rate_limiter)
        self._client = NBAStatsClient(cache=self.cache, rate_limiter=self.rate_limiter)  # type: ignore[arg-type]

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        player_id = int(entity_id)
        cache_key = f"common_player_info_{player_id}"
        cached = self.cache.get(cache_key)
        if cached:
            self.logger.debug("Cache hit for player bio", player_id=player_id)
            return cached  # type: ignore[return-value]

        self.rate_limiter.acquire()
        self.logger.info("Fetching player bio", player_id=player_id)
        raw = self._client.adapter.get_common_player_info(player_id=player_id)

        dataset = raw.get(_COMMON_PLAYER_INFO_DATASET, {})
        headers: list[str] = dataset.get("headers", [])
        rows: list[list[Any]] = dataset.get("data", [])
        info = dict(zip(headers, rows[0], strict=False)) if rows else {}

        payload: dict[str, Any] = {"info": info, "player_id": player_id}
        self.cache.set(cache_key, payload)
        return payload

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        info = raw.get("info", {})
        player_id = raw.get("player_id", 0)
        if not info:
            self.logger.warning("No CommonPlayerInfo data", player_id=player_id)
            return []

        try:
            height_str = str(info.get("HEIGHT", "") or "")
            weight_raw = info.get("WEIGHT")
            draft_year_raw = info.get("DRAFT_YEAR")
            draft_round_raw = info.get("DRAFT_ROUND")
            draft_number_raw = info.get("DRAFT_NUMBER")
            birthdate_raw = str(info.get("BIRTHDATE", "") or "")

            def _safe_int(val: Any) -> int | None:
                s = str(val) if val is not None else ""
                return int(s) if s.isdigit() else None

            model = PlayerBioCreate(
                player_id=int(info.get("PERSON_ID", player_id)),
                position=str(info.get("POSITION", "") or "") or None,
                height_inches=_parse_height_inches(height_str),
                weight_lbs=float(weight_raw) if weight_raw and str(weight_raw).isdigit() else None,
                birthdate=birthdate_raw[:10] or None,
                birthplace_city=str(info.get("BIRTHPLACE_CITY", "") or "") or None,
                birthplace_state=str(info.get("BIRTHPLACE_STATE", "") or "") or None,
                birthplace_country=str(info.get("BIRTHPLACE_COUNTRY", "") or "") or None,
                country=str(info.get("COUNTRY", "") or "") or None,
                college=str(info.get("SCHOOL", "") or "") or None,
                draft_year=_safe_int(draft_year_raw),
                draft_round=_safe_int(draft_round_raw),
                draft_number=_safe_int(draft_number_raw),
                bbref_id=str(info.get("BBREF_ID", "") or "") or None,
            )
            return [model]
        except (pydantic.ValidationError, ValueError) as exc:
            self.logger.warning(
                "PlayerBio validation failed",
                player_id=player_id,
                error=str(exc),
            )
            return []

    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        rows_affected = 0
        conn.execute("BEGIN")
        try:
            for item in model:
                b = cast("PlayerBioCreate", item)
                if not require_fk(conn, "player", "player_id", b.player_id):
                    self.logger.warning(
                        "Player FK missing for bio, skipping",
                        player_id=b.player_id,
                    )
                    continue
                # UPDATE only; INSERT was done by PlayerIngestor
                conn.execute(
                    """
                    UPDATE player SET
                        position          = COALESCE(?, position),
                        height_inches     = COALESCE(?, height_inches),
                        weight_lbs        = COALESCE(?, weight_lbs),
                        birthdate         = COALESCE(?, birthdate),
                        birthplace_city   = COALESCE(?, birthplace_city),
                        birthplace_state  = COALESCE(?, birthplace_state),
                        birthplace_country= COALESCE(?, birthplace_country),
                        country           = COALESCE(?, country),
                        college           = COALESCE(?, college),
                        draft_year        = COALESCE(?, draft_year),
                        draft_round       = COALESCE(?, draft_round),
                        draft_number      = COALESCE(?, draft_number),
                        bbref_id          = COALESCE(?, bbref_id)
                    WHERE player_id = ?
                    """,
                    (
                        b.position,
                        b.height_inches,
                        b.weight_lbs,
                        b.birthdate,
                        b.birthplace_city,
                        b.birthplace_state,
                        b.birthplace_country,
                        b.country,
                        b.college,
                        b.draft_year,
                        b.draft_round,
                        b.draft_number,
                        b.bbref_id,
                        b.player_id,
                    ),
                )
                changed: int = conn.execute("SELECT changes()").fetchone()[0]
                rows_affected += changed
            conn.execute("COMMIT")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        upsert_audit(conn, self.entity_type, "all", "nba_api", "SUCCESS", rows_affected)
        self.logger.info("Enriched player bios", rows_affected=rows_affected)
        return rows_affected
