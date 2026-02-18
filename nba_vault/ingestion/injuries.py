"""Injury data ingestor.

This ingestor fetches player injury data from various sources including
ESPN, NBA.com, and Rotowire. Since there's no official API for injuries,
we use web scraping with proper rate limiting and error handling.
"""

from datetime import date, datetime
from typing import Any

import pydantic
import requests
import structlog
from bs4 import BeautifulSoup

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.registry import register_ingestor
from nba_vault.models.advanced_stats import InjuryCreate

logger = structlog.get_logger(__name__)


@register_ingestor
class InjuryIngestor(BaseIngestor):
    """
    Ingestor for player injury data from various sources.

    Supports fetching injury data from:
    - ESPN NBA injuries page
    - NBA.com injuries (if available)
    - Rotowire NBA injuries

    Injury data includes:
    - Player name and team
    - Injury date
    - Injury type and body part
    - Status (out, day-to-day, questionable, etc.)
    - Games missed
    - Expected return date

    Note: Injury data availability varies by source and season.
    Historical injury data is often incomplete.
    """

    entity_type = "injuries"

    def __init__(self, cache=None, rate_limiter=None):
        """
        Initialize InjuryIngestor.

        Args:
            cache: Content cache for API responses.
            rate_limiter: Rate limiter for requests.
        """
        super().__init__(cache, rate_limiter)
        self.session = requests.Session()
        # Set a user agent to avoid being blocked
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
        )

    def fetch(
        self,
        entity_id: str,
        source: str = "espn",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Fetch injury data from various sources.

        Args:
            entity_id: "all" for all injuries, "team:<team_id>" for specific team,
                      or "player:<player_id>" for specific player.
            source: Data source ("espn", "rotowire", "nba").
            **kwargs: Additional parameters for the request.

        Returns:
            Dictionary with injury data.

        Raises:
            ValueError: If source is not supported.
            Exception: If fetch fails after retries.
        """
        if entity_id == "all":
            self.logger.info("Fetching all injuries", source=source)

            if source == "espn":
                injuries = self._fetch_espn_injuries()
            elif source == "rotowire":
                injuries = self._fetch_rotowire_injuries()
            elif source == "nba":
                injuries = self._fetch_nba_injuries()
            else:
                raise ValueError(f"Unsupported source: {source}")

            return {
                "scope": "all",
                "source": source,
                "injuries": injuries,
            }

        elif entity_id.startswith("team:"):
            team_abbreviation = entity_id.split(":")[1]
            self.logger.info("Fetching injuries for team", team=team_abbreviation, source=source)

            # Filter injuries by team after fetching all
            if source == "espn":
                injuries = self._fetch_espn_injuries()
            elif source == "rotowire":
                injuries = self._fetch_rotowire_injuries()
            else:
                injuries = []

            # Filter by team
            filtered_injuries = [
                inj for inj in injuries if inj.get("team", "").lower() == team_abbreviation.lower()
            ]

            return {
                "scope": "team",
                "team": team_abbreviation,
                "source": source,
                "injuries": filtered_injuries,
            }

        elif entity_id.startswith("player:"):
            player_name = entity_id.split(":", 1)[1]
            self.logger.info("Fetching injuries for player", player=player_name, source=source)

            # Would need to search for specific player
            # For now, return empty
            return {
                "scope": "player",
                "player": player_name,
                "source": source,
                "injuries": [],
            }

        else:
            raise ValueError(f"Invalid entity_id format: {entity_id}")

    def _fetch_espn_injuries(self) -> list[dict[str, Any]]:
        """
        Fetch injuries from ESPN NBA injuries page.

        Returns:
            List of injury dictionaries.

        Raises:
            Exception: If fetch fails.
        """
        url = "https://www.espn.com/nba/injuries"

        self.rate_limiter.acquire()

        try:
            self.logger.info("Fetching injuries from ESPN", url=url)
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")
            injuries = []

            # ESPN injury page structure varies, but typically has tables
            # Look for injury tables
            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                for row in rows[1:]:  # Skip header row
                    cols = row.find_all("td")
                    if len(cols) >= 3:
                        # Extract player info
                        player_cell = cols[0]
                        player_name = player_cell.get_text(strip=True)

                        # Extract team
                        team_cell = cols[1] if len(cols) > 1 else None
                        team = team_cell.get_text(strip=True) if team_cell else ""

                        # Extract status
                        status_cell = cols[2] if len(cols) > 2 else None
                        status = status_cell.get_text(strip=True) if status_cell else ""

                        # Extract injury description
                        desc_cell = cols[3] if len(cols) > 3 else None
                        injury_desc = desc_cell.get_text(strip=True) if desc_cell else ""

                        # Parse injury description for type and body part
                        injury_type, body_part = self._parse_injury_description(injury_desc)

                        # Extract date (if available)
                        date_cell = cols[4] if len(cols) > 4 else None
                        injury_date_str = date_cell.get_text(strip=True) if date_cell else ""
                        injury_date = self._parse_date(injury_date_str)

                        injuries.append(
                            {
                                "player_name": player_name,
                                "team": team,
                                "status": status,
                                "injury_type": injury_type,
                                "body_part": body_part,
                                "injury_date": injury_date,
                                "notes": injury_desc,
                            }
                        )

            self.logger.info("Fetched injuries from ESPN", count=len(injuries))
            return injuries

        except Exception as e:
            self.logger.error("Failed to fetch ESPN injuries", error=str(e))
            raise

    def _fetch_rotowire_injuries(self) -> list[dict[str, Any]]:
        """
        Fetch injuries from Rotowire NBA injuries page.

        Returns:
            List of injury dictionaries.

        Raises:
            Exception: If fetch fails.
        """
        url = "https://www.rotowire.com/basketball/nba-injuries.php"

        self.rate_limiter.acquire()

        try:
            self.logger.info("Fetching injuries from Rotowire", url=url)
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")
            injuries = []

            # Rotowire injury page structure
            # Look for injury listings
            injury_divs = soup.find_all("div", class_="lineup")
            for div in injury_divs:
                # Extract team
                team_header = div.find("span", class_="team-name")
                if not team_header:
                    continue
                team = team_header.get_text(strip=True)

                # Extract player rows
                player_rows = div.find_all("div", class_="player")
                for row in player_rows:
                    # Extract player name
                    player_link = row.find("a", class_="player-name")
                    if not player_link:
                        continue
                    player_name = player_link.get_text(strip=True)

                    # Extract status
                    status_span = row.find("span", class_="status")
                    status = status_span.get_text(strip=True) if status_span else ""

                    # Extract injury description
                    desc_div = row.find("div", class_="news")
                    injury_desc = desc_div.get_text(strip=True) if desc_div else ""

                    # Parse injury description
                    injury_type, body_part = self._parse_injury_description(injury_desc)

                    injuries.append(
                        {
                            "player_name": player_name,
                            "team": team,
                            "status": status,
                            "injury_type": injury_type,
                            "body_part": body_part,
                            "injury_date": date.today(),  # Rotowire doesn't always show date
                            "notes": injury_desc,
                        }
                    )

            self.logger.info("Fetched injuries from Rotowire", count=len(injuries))
            return injuries

        except Exception as e:
            self.logger.error("Failed to fetch Rotowire injuries", error=str(e))
            raise

    def _fetch_nba_injuries(self) -> list[dict[str, Any]]:
        """
        Fetch injuries from NBA.com (if available).

        Returns:
            List of injury dictionaries.

        Note: NBA.com doesn't have a dedicated public injury API.
        This method may need to be updated if NBA.com adds one.
        """
        # Placeholder for future NBA.com injury integration
        self.logger.warning("NBA.com injury fetch not yet implemented")
        return []

    def _parse_injury_description(self, desc: str) -> tuple[str | None, str | None]:
        """
        Parse injury description to extract injury type and body part.

        Args:
            desc: Injury description string.

        Returns:
            Tuple of (injury_type, body_part).
        """
        if not desc:
            return None, None

        desc_lower = desc.lower()

        # Common body parts
        body_parts = [
            "knee",
            "ankle",
            "foot",
            "heel",
            "toe",
            "hip",
            "groin",
            "thigh",
            "hamstring",
            "quad",
            "calf",
            "shin",
            "achilles",
            "back",
            "spine",
            "shoulder",
            "elbow",
            "wrist",
            "hand",
            "finger",
            "thumb",
            "head",
            "neck",
            "face",
            "eye",
            "nose",
            "concussion",
            "chest",
            "rib",
        ]

        # Common injury types
        injury_types = [
            "strain",
            "sprain",
            "fracture",
            "break",
            "tear",
            "rupture",
            "contusion",
            "bruise",
            "soreness",
            "inflammation",
            "tendinitis",
            "bursitis",
            "dislocation",
            "subluxation",
            "concussion",
        ]

        body_part = None
        for bp in body_parts:
            if bp in desc_lower:
                body_part = bp
                break

        injury_type = None
        for it in injury_types:
            if it in desc_lower:
                injury_type = it
                break

        return injury_type, body_part

    def _parse_date(self, date_str: str) -> date | None:
        """
        Parse date string into date object.

        Args:
            date_str: Date string in various formats.

        Returns:
            Date object or None if parsing fails.
        """
        if not date_str:
            return None

        # Try common date formats
        formats = [
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%m/%d/%y",
            "%B %d, %Y",
            "%b %d, %Y",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        return None

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        """
        Validate raw injury data using Pydantic models.

        Args:
            raw: Raw data dictionary with 'injuries' key containing list of injury dicts.

        Returns:
            List of validated InjuryCreate models.

        Raises:
            pydantic.ValidationError: If validation fails.
        """
        injuries_data = raw.get("injuries", [])

        validated_injuries = []

        for injury_data in injuries_data:
            try:
                # Extract player_id if available (would need player lookup)
                # For now, use player_name as identifier
                player_id = injury_data.get("player_id")  # This would need to be looked up
                if not player_id:
                    # Skip injuries without player_id for now
                    # In production, you'd look up the player_id from player_name
                    continue

                injury_record = {
                    "player_id": player_id,
                    "team_id": injury_data.get("team_id"),  # Would need team lookup
                    "injury_date": injury_data.get("injury_date", date.today()),
                    "injury_type": injury_data.get("injury_type"),
                    "body_part": injury_data.get("body_part"),
                    "status": injury_data.get("status", "Unknown"),
                    "games_missed": injury_data.get("games_missed", 0),
                    "return_date": injury_data.get("return_date"),
                    "notes": injury_data.get("notes"),
                }

                validated_injury = InjuryCreate(**injury_record)
                validated_injuries.append(validated_injury)

            except pydantic.ValidationError as e:
                self.logger.error(
                    "Injury validation failed",
                    injury_data=injury_data,
                    errors=str(e),
                )
                # Don't raise - skip invalid records
                continue

        self.logger.info("Validated injuries", count=len(validated_injuries))
        return validated_injuries

    def upsert(self, model: list[pydantic.BaseModel], conn) -> int:
        """
        Insert or update validated injury data in database.

        Args:
            model: List of validated InjuryCreate models.
            conn: SQLite database connection.

        Returns:
            Number of rows affected.
        """
        rows_affected = 0

        for injury in model:
            if not isinstance(injury, InjuryCreate):
                continue

            # Check if similar injury exists
            cursor = conn.execute(
                """
                SELECT injury_id FROM injury
                WHERE player_id = ? AND injury_date = ? AND status = ?
                """,
                (injury.player_id, injury.injury_date, injury.status),
            )
            existing = cursor.fetchone()

            if existing:
                # Update existing injury
                injury_id = existing[0]
                self._update_injury(injury_id, injury, conn)
                rows_affected += 1
            else:
                # Insert new injury
                self._insert_injury(injury, conn)
                rows_affected += 1

            # Log to ingestion_audit
            conn.execute(
                """
                INSERT INTO ingestion_audit
                (entity_type, entity_id, status, source, metadata, ingested_at)
                VALUES (?, ?, 'SUCCESS', ?, ?, datetime('now'))
                """,
                (
                    self.entity_type,
                    str(injury.player_id),
                    "web_scraping",
                    f"player: {injury.player_id}, date: {injury.injury_date}",
                ),
            )

        return rows_affected

    def _insert_injury(self, injury: InjuryCreate, conn) -> None:
        """
        Insert a new injury into the database.

        Args:
            injury: InjuryCreate model.
            conn: SQLite database connection.
        """
        conn.execute(
            """
            INSERT INTO injury (
                player_id, team_id, injury_date, injury_type, body_part,
                status, games_missed, return_date, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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

    def _update_injury(self, injury_id: int, injury: InjuryCreate, conn) -> None:
        """
        Update an existing injury in the database.

        Args:
            injury_id: Database ID of the injury.
            injury: InjuryCreate model.
            conn: SQLite database connection.
        """
        conn.execute(
            """
            UPDATE injury SET
                team_id = ?, injury_date = ?, injury_type = ?, body_part = ?,
                status = ?, games_missed = ?, return_date = ?, notes = ?
            WHERE injury_id = ?
            """,
            (
                injury.team_id,
                injury.injury_date,
                injury.injury_type,
                injury.body_part,
                injury.status,
                injury.games_missed,
                injury.return_date,
                injury.notes,
                injury_id,
            ),
        )
