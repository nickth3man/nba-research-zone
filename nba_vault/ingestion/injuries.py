"""Injury data ingestor.

Fetches player injury data from web-scraped sources (ESPN, Rotowire).
Player names from scrapers are resolved to database player_ids using
exact match first, then fuzzy matching via difflib.SequenceMatcher
(threshold ≥ 0.85).

Historical injury data is inherently incomplete; records that cannot be
resolved to a player_id are silently skipped and logged at DEBUG level.
"""

from __future__ import annotations

import difflib
from datetime import date
from typing import Any

import pydantic
import requests
import structlog

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.registry import register_ingestor
from nba_vault.ingestion.scrapers.injury_scrapers import (
    ESPNInjuryScraper,
    RotowireInjuryScraper,
)
from nba_vault.models.advanced_stats import InjuryCreate

logger = structlog.get_logger(__name__)

_FUZZY_CUTOFF = 0.85


@register_ingestor
class InjuryIngestor(BaseIngestor):
    """
    Ingestor for player injury data from ESPN and Rotowire.

    entity_id:  "all" | "team:<abbr>" | "player:<name>"
    kwargs:
        source (str): "espn" (default) | "rotowire"

    Usage:
        ingestor = InjuryIngestor()
        result = ingestor.ingest("all", conn, source="espn")
    """

    entity_type = "injuries"

    def __init__(self, cache: Any = None, rate_limiter: Any = None) -> None:
        super().__init__(cache, rate_limiter)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/91.0.4472.124 Safari/537.36"
                )
            }
        )
        self.espn_scraper = ESPNInjuryScraper(self.rate_limiter, self.session)
        self.rotowire_scraper = RotowireInjuryScraper(self.rate_limiter, self.session)

    # ------------------------------------------------------------------
    # Override ingest() to inject the player name→ID map before validate
    # ------------------------------------------------------------------

    def ingest(self, entity_id: str, conn: Any, **kwargs: Any) -> dict[str, Any]:
        """
        Full pipeline with player-name resolution injected before validate().

        Overrides BaseIngestor.ingest() to build a player name→ID lookup from
        the SQLite `player` table, so that validate() can resolve scraped
        player names to integer player_ids without a separate DB query.
        """
        raw = self.fetch(entity_id, **kwargs)
        raw["_player_name_map"] = self._build_player_name_map(conn)
        validated = self.validate(raw)
        rows_affected = self.upsert(validated, conn)
        source = kwargs.get("source", "espn")
        self.logger.info(
            "Injury ingest complete",
            entity_id=entity_id,
            source=source,
            rows_affected=rows_affected,
        )
        return {
            "status": "SUCCESS",
            "entity_id": entity_id,
            "rows_affected": rows_affected,
        }

    # ------------------------------------------------------------------
    # fetch / validate / upsert
    # ------------------------------------------------------------------

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        source: str = str(kwargs.get("source", "espn"))

        if entity_id == "all":
            self.logger.info("Fetching all injuries", source=source)
            injuries = self._scrape(source)
            return {"scope": "all", "source": source, "injuries": injuries}

        if entity_id.startswith("team:"):
            abbr = entity_id.split(":", 1)[1]
            all_injuries = self._scrape(source)
            filtered = [inj for inj in all_injuries if inj.get("team", "").lower() == abbr.lower()]
            return {"scope": "team", "team": abbr, "source": source, "injuries": filtered}

        if entity_id.startswith("player:"):
            name = entity_id.split(":", 1)[1]
            all_injuries = self._scrape(source)
            filtered = [
                inj for inj in all_injuries if inj.get("player_name", "").lower() == name.lower()
            ]
            return {"scope": "player", "player": name, "source": source, "injuries": filtered}

        raise ValueError(f"Invalid entity_id format: {entity_id!r}")

    def _scrape(self, source: str) -> list[dict[str, Any]]:
        if source == "espn":
            return self.espn_scraper.fetch()  # type: ignore[return-value]
        if source == "rotowire":
            return self.rotowire_scraper.fetch()  # type: ignore[return-value]
        raise ValueError(f"Unsupported injury source: {source!r}")

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        name_map: dict[str, int] = raw.get("_player_name_map", {})
        validated: list[pydantic.BaseModel] = []

        for injury_data in raw.get("injuries", []):
            try:
                # Try to resolve player_id
                player_id: int | None = injury_data.get("player_id")
                if not player_id:
                    player_name = str(injury_data.get("player_name", "") or "").strip()
                    if player_name:
                        player_id = self._resolve_player_id(player_name, name_map)
                if not player_id:
                    self.logger.debug(
                        "Cannot resolve player_id for injury; skipping",
                        player_name=injury_data.get("player_name"),
                    )
                    continue

                injury_record = InjuryCreate(
                    player_id=player_id,
                    team_id=injury_data.get("team_id"),
                    injury_date=injury_data.get("injury_date", date.today()),
                    injury_type=injury_data.get("injury_type"),
                    body_part=injury_data.get("body_part"),
                    status=injury_data.get("status", "Unknown"),
                    games_missed=injury_data.get("games_missed", 0),
                    return_date=injury_data.get("return_date"),
                    notes=injury_data.get("notes"),
                )
                validated.append(injury_record)

            except (pydantic.ValidationError, ValueError) as exc:
                self.logger.warning(
                    "Injury validation failed",
                    player_name=injury_data.get("player_name"),
                    error=str(exc),
                )

        self.logger.info("Validated injuries", count=len(validated))
        return validated

    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        rows_affected = 0
        for item in model:
            if not isinstance(item, InjuryCreate):
                continue
            injury = item
            cursor = conn.execute(
                """
                SELECT injury_id FROM injury
                WHERE player_id = ? AND injury_date = ? AND status = ?
                """,
                (injury.player_id, injury.injury_date, injury.status),
            )
            existing = cursor.fetchone()

            if existing:
                conn.execute(
                    """
                    UPDATE injury SET
                        team_id = ?, injury_type = ?, body_part = ?,
                        games_missed = ?, return_date = ?, notes = ?
                    WHERE injury_id = ?
                    """,
                    (
                        injury.team_id,
                        injury.injury_type,
                        injury.body_part,
                        injury.games_missed,
                        injury.return_date,
                        injury.notes,
                        existing[0],
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO injury
                        (player_id, team_id, injury_date, injury_type,
                         body_part, status, games_missed, return_date, notes)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        injury.player_id,
                        injury.team_id,
                        injury.injury_date,
                        injury.injury_type,
                        injury.body_part,
                        injury.status,
                        injury.games_missed,
                        injury.return_date,
                        injury.notes,
                    ),
                )
            rows_affected += 1

        conn.execute(
            """
            INSERT INTO ingestion_audit
                (entity_type, entity_id, status, source, ingest_ts, row_count)
            VALUES (?, ?, 'SUCCESS', 'web_scraping', datetime('now'), ?)
            ON CONFLICT(entity_type, entity_id, source) DO UPDATE SET
                ingest_ts = excluded.ingest_ts,
                status    = excluded.status,
                row_count = excluded.row_count
            """,
            (self.entity_type, "all", rows_affected),
        )
        self.logger.info("Upserted injuries", rows_affected=rows_affected)
        return rows_affected

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_player_name_map(self, conn: Any) -> dict[str, int]:
        """Build {full_name.lower(): player_id} from the player table."""
        try:
            cur = conn.execute(
                "SELECT player_id, full_name FROM player WHERE full_name IS NOT NULL"
            )
            return {row[1].strip().lower(): row[0] for row in cur.fetchall() if row[1]}
        except Exception as exc:
            self.logger.warning("Failed to build player name map", error=str(exc))
            return {}

    def _resolve_player_id(self, name: str, name_map: dict[str, int]) -> int | None:
        """Exact then fuzzy match a scraped player name to a DB player_id."""
        name_lower = name.strip().lower()
        # 1. Exact
        if name_lower in name_map:
            return name_map[name_lower]
        # 2. Fuzzy
        matches = difflib.get_close_matches(
            name_lower, list(name_map.keys()), n=1, cutoff=_FUZZY_CUTOFF
        )
        if matches:
            self.logger.debug(
                "Fuzzy-matched player name",
                scraped=name,
                matched=matches[0],
            )
            return name_map[matches[0]]
        return None

    def _parse_injury_description(self, desc: str | None) -> tuple[str | None, str | None]:
        return self.espn_scraper.parse_injury_description(desc)

    def _parse_date(self, date_str: str | None) -> date | None:
        return self.espn_scraper.parse_date(date_str)
