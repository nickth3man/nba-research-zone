"""Draft combine measurements ingestor.

Source: nba_api DraftCombineNonStatMeasures (anthropometric) +
        nba_api DraftCombineDrillResults (agility/athletic drills).
Populates `draft_combine` table.
Era gate: combine measurements available from 2000-01 onwards.
"""

from __future__ import annotations

import sqlite3
from typing import Any, cast

import pydantic
import structlog

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.nba_stats_client import NBAStatsClient
from nba_vault.ingestion.registry import register_ingestor
from nba_vault.ingestion.validation import check_data_availability, upsert_audit
from nba_vault.models.entities import DraftCombineAnthroCreate

logger = structlog.get_logger(__name__)

# Both DraftCombinePlayerAnthro and DraftCombineDrillResults return a single
# dataset named "Results" via get_normalized_dict() in the installed nba_api version.
_ANTHRO_DATASET = "Results"
_DRILLS_DATASET = "Results"


@register_ingestor
class DraftCombineIngestor(BaseIngestor):
    """
    Ingestor for NBA Draft Combine anthropometric + drill measurements.

    entity_id convention: "<draft_year>" e.g. "2023"
    kwargs:
        (none required beyond entity_id)

    Usage:
        ingestor = DraftCombineIngestor()
        result = ingestor.ingest("2023", conn)
    """

    entity_type = "draft_combine"

    def __init__(self, cache: Any = None, rate_limiter: Any = None) -> None:
        super().__init__(cache, rate_limiter)
        self._client = NBAStatsClient(cache=self.cache, rate_limiter=self.rate_limiter)  # type: ignore[arg-type]

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        draft_year = int(entity_id)
        check_data_availability("draft_combine", draft_year)

        cache_key = f"draft_combine_{draft_year}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached  # type: ignore[return-value]

        # Adapter expects season_year as the 4-digit year string e.g. "2023"
        season_year_str = str(draft_year)

        self.rate_limiter.acquire()
        self.logger.info("Fetching draft combine anthro", draft_year=draft_year)
        anthro_raw = self._client.adapter.get_draft_combine_anthro(season_year=season_year_str)

        self.rate_limiter.acquire()
        self.logger.info("Fetching draft combine drills", draft_year=draft_year)
        drills_raw = self._client.adapter.get_draft_combine_drills(season_year=season_year_str)

        # Parse anthropometric data
        ds_a = anthro_raw.get(_ANTHRO_DATASET, {})
        hdrs_a: list[str] = ds_a.get("headers", [])
        data_a: list[list[Any]] = ds_a.get("data", [])
        anthro_rows = [dict(zip(hdrs_a, row, strict=False)) for row in data_a]

        # Parse drill data — key by player_id for merging
        ds_d = drills_raw.get(_DRILLS_DATASET, {})
        hdrs_d: list[str] = ds_d.get("headers", [])
        data_d: list[list[Any]] = ds_d.get("data", [])
        drill_by_pid: dict[int, dict[str, Any]] = {}
        for row in data_d:
            d = dict(zip(hdrs_d, row, strict=False))
            pid = int(d.get("PLAYER_ID", 0) or 0)
            if pid:
                drill_by_pid[pid] = d

        payload: dict[str, Any] = {
            "anthro": anthro_rows,
            "drills": drill_by_pid,
            "draft_year": draft_year,
        }
        self.cache.set(cache_key, payload)
        self.logger.info(
            "Fetched combine data",
            draft_year=draft_year,
            anthro_count=len(anthro_rows),
            drill_count=len(drill_by_pid),
        )
        return payload

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        draft_year = int(raw.get("draft_year", 0))
        drill_by_pid: dict[int, dict[str, Any]] = raw.get("drills", {})
        validated: list[pydantic.BaseModel] = []
        for row in raw.get("anthro", []):
            try:
                pid = int(row.get("PLAYER_ID", 0) or 0)
                if pid == 0:
                    continue
                drills = drill_by_pid.get(pid, {})
                model = DraftCombineAnthroCreate(
                    player_id=pid,
                    draft_year=draft_year,
                    height_wo_shoes_inches=_safe_float(row.get("HEIGHT_WO_SHOES")),
                    height_w_shoes_inches=_safe_float(row.get("HEIGHT_W_SHOES")),
                    wingspan_inches=_safe_float(row.get("WINGSPAN")),
                    standing_reach_inches=_safe_float(row.get("STANDING_REACH")),
                    weight_lbs=_safe_float(row.get("WEIGHT")),
                    body_fat_pct=_safe_float(row.get("BODY_FAT_PCT")),
                    hand_length_inches=_safe_float(row.get("HAND_LENGTH")),
                    hand_width_inches=_safe_float(row.get("HAND_WIDTH")),
                    vertical_leap_standing_inches=_safe_float(drills.get("STANDING_VERTICAL_LEAP")),
                    vertical_leap_max_inches=_safe_float(drills.get("MAX_VERTICAL_LEAP")),
                    lane_agility_time_sec=_safe_float(drills.get("LANE_AGILITY_TIME")),
                    three_quarter_sprint_sec=_safe_float(drills.get("THREE_QUARTER_SPRINT")),
                    bench_press_reps=_safe_int(drills.get("BENCH_PRESS")),
                )
                validated.append(model)
            except (pydantic.ValidationError, ValueError) as exc:
                if isinstance(exc, pydantic.ValidationError):
                    self.logger.warning(
                        "combine_validation_error",
                        player_id=row.get("PLAYER_ID"),
                        error_count=len(exc.errors()),
                        errors=exc.errors(),
                    )
                else:
                    self.logger.warning(
                        "combine_validation_error",
                        player_id=row.get("PLAYER_ID"),
                        error=str(exc),
                        exc_info=True,
                    )
        self.logger.info("Validated combine rows", count=len(validated))
        return validated

    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        rows_affected = 0
        draft_year_label = ""
        conn.execute("BEGIN")
        try:
            for item in model:
                m = cast("DraftCombineAnthroCreate", item)
                draft_year_label = str(m.draft_year)
                conn.execute(
                    """
                    INSERT INTO draft_combine
                        (player_id, draft_year,
                         height_wo_shoes_inches, height_w_shoes_inches,
                         wingspan_inches, standing_reach_inches,
                         weight_lbs, body_fat_pct,
                         hand_length_inches, hand_width_inches,
                         vertical_leap_standing_inches, vertical_leap_max_inches,
                         lane_agility_time_sec, three_quarter_sprint_sec,
                         bench_press_reps)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(player_id, draft_year) DO UPDATE SET
                        height_wo_shoes_inches          = excluded.height_wo_shoes_inches,
                        height_w_shoes_inches           = excluded.height_w_shoes_inches,
                        wingspan_inches                 = excluded.wingspan_inches,
                        standing_reach_inches           = excluded.standing_reach_inches,
                        weight_lbs                      = excluded.weight_lbs,
                        body_fat_pct                    = excluded.body_fat_pct,
                        hand_length_inches              = excluded.hand_length_inches,
                        hand_width_inches               = excluded.hand_width_inches,
                        vertical_leap_standing_inches   = excluded.vertical_leap_standing_inches,
                        vertical_leap_max_inches        = excluded.vertical_leap_max_inches,
                        lane_agility_time_sec           = excluded.lane_agility_time_sec,
                        three_quarter_sprint_sec        = excluded.three_quarter_sprint_sec,
                        bench_press_reps                = excluded.bench_press_reps
                    """,
                    (
                        m.player_id,
                        m.draft_year,
                        m.height_wo_shoes_inches,
                        m.height_w_shoes_inches,
                        m.wingspan_inches,
                        m.standing_reach_inches,
                        m.weight_lbs,
                        m.body_fat_pct,
                        m.hand_length_inches,
                        m.hand_width_inches,
                        m.vertical_leap_standing_inches,
                        m.vertical_leap_max_inches,
                        m.lane_agility_time_sec,
                        m.three_quarter_sprint_sec,
                        m.bench_press_reps,
                    ),
                )
                rows_affected += 1
            conn.execute("COMMIT")
        except sqlite3.Error as exc:
            conn.execute("ROLLBACK")
            self.logger.exception(
                "combine_upsert_failed",
                draft_year=draft_year_label,
                rows_before_error=rows_affected,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            raise

        upsert_audit(
            conn, self.entity_type, draft_year_label or "all", "nba_api", "SUCCESS", rows_affected
        )
        self.logger.info("Upserted combine rows", rows_affected=rows_affected)
        return rows_affected


def _safe_float(val: Any) -> float | None:
    if val is None or str(val).strip() in ("", "None", "null"):
        return None
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None


def _safe_int(val: Any) -> int | None:
    if val is None or str(val).strip() in ("", "None", "null"):
        return None
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None
