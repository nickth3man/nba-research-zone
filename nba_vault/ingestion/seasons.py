"""Season and franchise data ingestors.

Sources:
- SeasonIngestor: nba_api LeagueGameFinder (derives distinct seasons from game data)
  with a static seed for all 78 seasons 1946-47 → present.
- FranchiseIngestor: nba_api FranchiseHistory endpoint.
"""

from __future__ import annotations

import sqlite3
from typing import Any, cast

import pydantic
import structlog

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.registry import register_ingestor
from nba_vault.ingestion.validation import upsert_audit
from nba_vault.models.entities import FranchiseCreate, SeasonCreate

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Static season seed — all 78 NBA/ABA/BAA seasons.
# Used as fallback and to populate seasons that predate digital API coverage.
# Format: (season_id, league_id, season_label, games_per_team)  # noqa: ERA001
# ---------------------------------------------------------------------------
_SEASON_SEED: list[tuple[int, str, str, int | None]] = [
    # BAA era
    (1946, "BAA", "1946-47", 60),
    (1947, "BAA", "1947-48", 48),
    (1948, "BAA", "1948-49", 60),
    # NBA era begins 1949-50
    (1949, "NBA", "1949-50", 68),
    (1950, "NBA", "1950-51", 68),
    (1951, "NBA", "1951-52", 66),
    (1952, "NBA", "1952-53", 70),
    (1953, "NBA", "1953-54", 72),
    (1954, "NBA", "1954-55", 72),
    (1955, "NBA", "1955-56", 72),
    (1956, "NBA", "1956-57", 72),
    (1957, "NBA", "1957-58", 72),
    (1958, "NBA", "1958-59", 72),
    (1959, "NBA", "1959-60", 75),
    (1960, "NBA", "1960-61", 79),
    (1961, "NBA", "1961-62", 80),
    (1962, "NBA", "1962-63", 80),
    (1963, "NBA", "1963-64", 80),
    (1964, "NBA", "1964-65", 80),
    (1965, "NBA", "1965-66", 80),
    (1966, "NBA", "1966-67", 81),
    (1967, "NBA", "1967-68", 82),
    (1968, "NBA", "1968-69", 82),
    (1969, "NBA", "1969-70", 82),
    (1970, "NBA", "1970-71", 82),
    (1971, "NBA", "1971-72", 82),
    (1972, "NBA", "1972-73", 82),
    (1973, "NBA", "1973-74", 82),
    (1974, "NBA", "1974-75", 82),
    (1975, "NBA", "1975-76", 82),
    (1976, "NBA", "1976-77", 82),
    (1977, "NBA", "1977-78", 82),
    (1978, "NBA", "1978-79", 82),
    (1979, "NBA", "1979-80", 82),
    (1980, "NBA", "1980-81", 82),
    (1981, "NBA", "1981-82", 82),
    (1982, "NBA", "1982-83", 82),
    (1983, "NBA", "1983-84", 82),
    (1984, "NBA", "1984-85", 82),
    (1985, "NBA", "1985-86", 82),
    (1986, "NBA", "1986-87", 82),
    (1987, "NBA", "1987-88", 82),
    (1988, "NBA", "1988-89", 82),
    (1989, "NBA", "1989-90", 82),
    (1990, "NBA", "1990-91", 82),
    (1991, "NBA", "1991-92", 82),
    (1992, "NBA", "1992-93", 82),
    (1993, "NBA", "1993-94", 82),
    (1994, "NBA", "1994-95", 82),
    (1995, "NBA", "1995-96", 82),
    (1996, "NBA", "1996-97", 82),
    (1997, "NBA", "1997-98", 82),
    (1998, "NBA", "1998-99", 50),  # Lockout-shortened
    (1999, "NBA", "1999-00", 82),
    (2000, "NBA", "2000-01", 82),
    (2001, "NBA", "2001-02", 82),
    (2002, "NBA", "2002-03", 82),
    (2003, "NBA", "2003-04", 82),
    (2004, "NBA", "2004-05", 82),
    (2005, "NBA", "2005-06", 82),
    (2006, "NBA", "2006-07", 82),
    (2007, "NBA", "2007-08", 82),
    (2008, "NBA", "2008-09", 82),
    (2009, "NBA", "2009-10", 82),
    (2010, "NBA", "2010-11", 82),
    (2011, "NBA", "2011-12", 66),  # Lockout-shortened
    (2012, "NBA", "2012-13", 82),
    (2013, "NBA", "2013-14", 82),
    (2014, "NBA", "2014-15", 82),
    (2015, "NBA", "2015-16", 82),
    (2016, "NBA", "2016-17", 82),
    (2017, "NBA", "2017-18", 82),
    (2018, "NBA", "2018-19", 82),
    (2019, "NBA", "2019-20", 72),  # Bubble season
    (2020, "NBA", "2020-21", 72),
    (2021, "NBA", "2021-22", 82),
    (2022, "NBA", "2022-23", 82),
    (2023, "NBA", "2023-24", 82),
    (2024, "NBA", "2024-25", 82),
    # ABA seasons (1967-76)
    (1967, "ABA", "1967-68", 78),
    (1968, "ABA", "1968-69", 78),
    (1969, "ABA", "1969-70", 84),
    (1970, "ABA", "1970-71", 84),
    (1971, "ABA", "1971-72", 84),
    (1972, "ABA", "1972-73", 84),
    (1973, "ABA", "1973-74", 84),
    (1974, "ABA", "1974-75", 84),
    (1975, "ABA", "1975-76", 84),
]


@register_ingestor
class SeasonIngestor(BaseIngestor):
    """
    Ingestor for NBA/ABA/BAA season metadata.

    Primary source: static seed list (covers all 78+ seasons including ABA era).
    The season table is a prerequisite for virtually all other ingestors because
    almost every other table has a season_id FK.

    Usage:
        ingestor = SeasonIngestor()
        result = ingestor.ingest("all", conn)
    """

    entity_type = "seasons"

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        """Return the static season seed as raw data."""
        self.logger.info("Loading season seed data", count=len(_SEASON_SEED))
        return {"seasons": _SEASON_SEED}

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        validated = []
        for row in raw.get("seasons", []):
            season_id, league_id, season_label, games_per_team = row
            try:
                model = SeasonCreate(
                    season_id=season_id,
                    league_id=league_id,
                    season_label=season_label,
                    games_per_team=games_per_team,
                )
                validated.append(model)
            except pydantic.ValidationError as e:
                self.logger.warning(
                    "Season validation failed",
                    season_id=season_id,
                    error=str(e),
                )
        self.logger.info("Validated seasons", count=len(validated))
        return validated

    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        rows_affected = 0
        conn.execute("BEGIN")
        try:
            for season in model:
                s = cast("SeasonCreate", season)
                conn.execute(
                    """
                    INSERT INTO season
                        (season_id, league_id, season_label, games_per_team)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(season_id) DO UPDATE SET
                        league_id      = excluded.league_id,
                        season_label   = excluded.season_label,
                        games_per_team = excluded.games_per_team
                    """,
                    (s.season_id, s.league_id, s.season_label, s.games_per_team),
                )
                rows_affected += 1
            conn.execute("COMMIT")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        upsert_audit(conn, self.entity_type, "all", "static_seed", "SUCCESS", rows_affected)
        self.logger.info("Upserted seasons", rows_affected=rows_affected)
        return rows_affected


@register_ingestor
class FranchiseIngestor(BaseIngestor):
    """
    Ingestor for NBA franchise history from the nba_api FranchiseHistory endpoint.

    A single call to FranchiseHistory returns all franchises (active and historical).
    Each row represents a continuous stint under one team name/city before a
    relocation or rename.

    Usage:
        ingestor = FranchiseIngestor()
        result = ingestor.ingest("all", conn)
    """

    entity_type = "franchises"

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        """Fetch all franchise history from nba_api."""
        cache_key = "nba_api_franchise_history"
        cached = self.cache.get(cache_key)
        if cached:
            self.logger.debug("Cache hit for franchise history")
            return cached

        self.rate_limiter.acquire()
        try:
            from nba_api.stats.endpoints import franchisehistory  # noqa: PLC0415

            self.logger.info("Fetching franchise history from NBA.com API")
            result = franchisehistory.FranchiseHistory(league_id="00")
            data = result.get_normalized_dict()

            # FranchiseHistory returns two datasets:
            #   FranchiseHistory — active and historical franchises
            #   DefunctTeams — teams that no longer exist
            franchises = data.get("FranchiseHistory", [])
            defunct = data.get("DefunctTeams", [])
            all_franchises = franchises + defunct

            payload = {"franchises": all_franchises}
            self.cache.set(cache_key, payload)
            self.logger.info("Fetched franchises", count=len(all_franchises))
            return payload
        except Exception as e:
            self.logger.error("Failed to fetch franchise history", error=str(e))
            raise

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        validated = []
        for row in raw.get("franchises", []):
            try:
                # API field names vary; normalise common variants
                franchise_id = row.get("TEAM_ID") or row.get("FRANCHISE_ID") or 0
                league_id_raw = row.get("LEAGUE_ID", "00")
                # Map NBA.com league codes to our canonical strings
                league_map = {"00": "NBA", "01": "ABA", "02": "BAA"}
                league_id = league_map.get(str(league_id_raw), "NBA")

                model = FranchiseCreate(
                    franchise_id=int(franchise_id),
                    nba_franchise_id=int(franchise_id),
                    current_team_name=row.get("TEAM_NAME", "Unknown"),
                    current_city=row.get("TEAM_CITY", ""),
                    abbreviation=row.get("TEAM_ABBREVIATION", "UNK"),
                    conference=row.get("CONFERENCE"),
                    division=row.get("DIVISION"),
                    founded_year=row.get("START_YEAR"),
                    league_id=league_id,
                )
                validated.append(model)
            except pydantic.ValidationError as e:
                self.logger.warning(
                    "Franchise validation failed",
                    row=row,
                    error=str(e),
                )
        self.logger.info("Validated franchises", count=len(validated))
        return validated

    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        rows_affected = 0
        conn.execute("BEGIN")
        try:
            for franchise in model:
                f = cast("FranchiseCreate", franchise)
                conn.execute(
                    """
                    INSERT INTO franchise
                        (franchise_id, nba_franchise_id, current_team_name, current_city,
                         abbreviation, conference, division, founded_year, league_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(franchise_id) DO UPDATE SET
                        nba_franchise_id  = excluded.nba_franchise_id,
                        current_team_name = excluded.current_team_name,
                        current_city      = excluded.current_city,
                        abbreviation      = excluded.abbreviation,
                        conference        = excluded.conference,
                        division          = excluded.division,
                        founded_year      = excluded.founded_year,
                        league_id         = excluded.league_id
                    """,
                    (
                        f.franchise_id,
                        f.nba_franchise_id,
                        f.current_team_name,
                        f.current_city,
                        f.abbreviation,
                        f.conference,
                        f.division,
                        f.founded_year,
                        f.league_id,
                    ),
                )
                rows_affected += 1
            conn.execute("COMMIT")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
            raise

        upsert_audit(conn, self.entity_type, "all", "nba_api", "SUCCESS", rows_affected)
        self.logger.info("Upserted franchises", rows_affected=rows_affected)
        return rows_affected
