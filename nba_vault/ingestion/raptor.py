"""RAPTOR player ratings ingestor.

Source: fivethirtyeight/nba-player-advanced-metrics (CC BY 4.0).
Covers every player season from 1976-77 (post-ABA merger) to present.

Three RAPTOR versions by era:
  'modern'  (2014+): full player tracking + on/off + box score
  'mixed'   (2001-2013): box score + single-year regularized plus-minus
  'box'     (1977-2000): box score estimate only

Downloads one CSV file:
  nba-data-historical.csv  — per player/season (1976-present)

Populates the `player_raptor` table.

Usage:
    ingestor = RaptorIngestor()
    result = ingestor.ingest("all", conn)
"""

from __future__ import annotations

import csv
import io
import sqlite3
import urllib.request
from typing import Any, cast

import pydantic
import structlog

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.registry import register_ingestor
from nba_vault.ingestion.validation import upsert_audit
from nba_vault.models.entities import PlayerRaptorCreate

logger = structlog.get_logger(__name__)

_HISTORICAL_URL = (
    "https://raw.githubusercontent.com/fivethirtyeight/nba-player-advanced-metrics"
    "/master/nba-data-historical.csv"
)


@register_ingestor
class RaptorIngestor(BaseIngestor):
    """
    Ingestor for FiveThirtyEight RAPTOR player ratings (1976-present).

    Downloads historical_RAPTOR_by_player.csv (~5 MB) and bulk-inserts
    into player_raptor. Resolves player_id via bbref_id on the player table.
    Idempotent via ON CONFLICT DO UPDATE.

    entity_id convention: "all" (single bulk load)

    Usage:
        ingestor = RaptorIngestor()
        result = ingestor.ingest("all", conn)
    """

    entity_type = "raptor_ratings"

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        cache_key = "raptor_historical_csv"
        cached = self.cache.get(cache_key)
        if cached:
            self.logger.debug("Cache hit for RAPTOR CSV")
            return cached  # type: ignore[return-value]

        self.logger.info("Downloading historical RAPTOR CSV", url=_HISTORICAL_URL)
        with urllib.request.urlopen(_HISTORICAL_URL, timeout=120) as resp:  # noqa: S310
            content = resp.read().decode("utf-8")

        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)
        self.logger.info("Downloaded RAPTOR rows", count=len(rows))

        payload: dict[str, Any] = {"rows": rows}
        self.cache.set(cache_key, payload)
        return payload

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        # Column mapping for nba-data-historical.csv:
        # player_id → bbref_id, year_id → season end year, type → RS/PO,
        # team_id → bbref_team_id, Min → mp, G → games,
        # "Raptor O" → raptor_offense, "Raptor D" → raptor_defense,
        # "Raptor+/-" → raptor_total, "Raptor WAR" → war_total
        validated: list[pydantic.BaseModel] = []
        for row in raw.get("rows", []):
            try:
                # year_id is the season end year (e.g. 2020 for 2019-20)
                year_id = _safe_int(row.get("year_id"))
                if year_id is None:
                    continue
                season_id = year_id - 1  # convert to start year

                season_type = str(row.get("type", "RS")).strip().upper()
                if season_type not in ("RS", "PO"):
                    season_type = "RS"

                bbref_id = str(row.get("player_id", "")).strip() or None
                if not bbref_id:
                    continue

                bbref_team_id = str(row.get("team_id", "")).strip() or None

                # Determine RAPTOR version by era
                if year_id >= 2014:
                    raptor_version = "modern"
                elif year_id >= 2001:
                    raptor_version = "mixed"
                else:
                    raptor_version = "box"

                model = PlayerRaptorCreate(
                    bbref_id=bbref_id,
                    season_id=season_id,
                    season_type=season_type,
                    bbref_team_id=bbref_team_id,
                    mp=_safe_int(row.get("Min")),
                    # Combined RAPTOR (no box/on-off split in this file)
                    raptor_offense=_safe_float(row.get("Raptor O")),
                    raptor_defense=_safe_float(row.get("Raptor D")),
                    raptor_total=_safe_float(row.get("Raptor+/-")),
                    war_total=_safe_float(row.get("Raptor WAR")),
                    raptor_version=raptor_version,
                )
                validated.append(model)
            except (pydantic.ValidationError, ValueError, KeyError) as exc:
                self.logger.debug("RAPTOR row validation error", error=str(exc))
        self.logger.info("Validated RAPTOR rows", count=len(validated))
        return validated

    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        # Build bbref_id → player_id lookup once for the whole batch
        bbref_lookup: dict[str, int] = {}
        try:
            cur = conn.execute("SELECT bbref_id, player_id FROM player WHERE bbref_id IS NOT NULL")
            for row in cur.fetchall():
                bbref_lookup[row[0]] = row[1]
        except sqlite3.Error as e:
            self.logger.warning("Failed to build bbref lookup", error=str(e))

        # Build set of valid season_ids to avoid FK constraint failures when
        # seasons table is not yet populated (Stage 0 runs before Stage 1).
        try:
            valid_seasons: set[int] = {
                r[0] for r in conn.execute("SELECT season_id FROM season").fetchall()
            }
        except sqlite3.Error:
            valid_seasons = set()

        skipped_no_season = 0
        rows_affected = 0
        conn.execute("BEGIN")
        try:
            for item in model:
                r = cast("PlayerRaptorCreate", item)
                # Skip rows where season_id is not in the season table (NOT NULL FK)
                if r.season_id not in valid_seasons:
                    skipped_no_season += 1
                    continue
                player_id = bbref_lookup.get(r.bbref_id) if r.bbref_id else None
                conn.execute(
                    """
                    INSERT INTO player_raptor
                        (player_id, bbref_id, season_id, season_type, bbref_team_id,
                         poss, mp,
                         raptor_box_offense, raptor_box_defense, raptor_box_total,
                         raptor_onoff_offense, raptor_onoff_defense, raptor_onoff_total,
                         raptor_offense, raptor_defense, raptor_total,
                         war_total, war_reg_season, war_playoffs,
                         predator_offense, predator_defense, predator_total,
                         pace_impact, raptor_version)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(bbref_id, season_id, season_type, bbref_team_id) DO UPDATE SET
                        player_id            = excluded.player_id,
                        poss                 = excluded.poss,
                        mp                   = excluded.mp,
                        raptor_box_offense   = excluded.raptor_box_offense,
                        raptor_box_defense   = excluded.raptor_box_defense,
                        raptor_box_total     = excluded.raptor_box_total,
                        raptor_onoff_offense = excluded.raptor_onoff_offense,
                        raptor_onoff_defense = excluded.raptor_onoff_defense,
                        raptor_onoff_total   = excluded.raptor_onoff_total,
                        raptor_offense       = excluded.raptor_offense,
                        raptor_defense       = excluded.raptor_defense,
                        raptor_total         = excluded.raptor_total,
                        war_total            = excluded.war_total,
                        war_reg_season       = excluded.war_reg_season,
                        war_playoffs         = excluded.war_playoffs,
                        predator_offense     = excluded.predator_offense,
                        predator_defense     = excluded.predator_defense,
                        predator_total       = excluded.predator_total,
                        pace_impact          = excluded.pace_impact,
                        raptor_version       = excluded.raptor_version
                    """,
                    (
                        player_id,
                        r.bbref_id,
                        r.season_id,
                        r.season_type,
                        r.bbref_team_id,
                        r.poss,
                        r.mp,
                        r.raptor_box_offense,
                        r.raptor_box_defense,
                        r.raptor_box_total,
                        r.raptor_onoff_offense,
                        r.raptor_onoff_defense,
                        r.raptor_onoff_total,
                        r.raptor_offense,
                        r.raptor_defense,
                        r.raptor_total,
                        r.war_total,
                        r.war_reg_season,
                        r.war_playoffs,
                        r.predator_offense,
                        r.predator_defense,
                        r.predator_total,
                        r.pace_impact,
                        r.raptor_version,
                    ),
                )
                rows_affected += 1
            conn.execute("COMMIT")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        upsert_audit(
            conn, self.entity_type, "all", "fivethirtyeight_raptor", "SUCCESS", rows_affected
        )
        self.logger.info(
            "Upserted RAPTOR rows",
            rows_affected=rows_affected,
            skipped_no_season=skipped_no_season,
        )
        return rows_affected


def _safe_float(val: Any) -> float | None:
    if val is None or str(val).strip() in ("", "None", "null", "NA"):
        return None
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None


def _safe_int(val: Any) -> int | None:
    if val is None or str(val).strip() in ("", "None", "null", "NA"):
        return None
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None
