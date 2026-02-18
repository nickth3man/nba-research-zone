"""Web scrapers for injury data from various sources.

This module provides specialized scrapers for fetching injury data from
different websites. Each scraper handles the unique HTML structure and
data format of its source.

Common functionality is provided by BaseInjuryScraper:
- HTML parsing utilities
- Date parsing (scraped dates vary by source)
- Injury description parsing
- Team name normalization
"""

from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Any

import structlog
from bs4 import BeautifulSoup

logger = structlog.get_logger(__name__)


class BaseInjuryScraper(ABC):
    """
    Abstract base class for injury scrapers.

    Provides common utilities for parsing injury data from web sources.
    Subclasses must implement the fetch() method to handle source-specific logic.
    """

    def __init__(self, rate_limiter, session):
        """
        Initialize scraper.

        Args:
            rate_limiter: Rate limiter for requests.
            session: requests.Session instance for making HTTP requests.
        """
        self.rate_limiter = rate_limiter
        self.session = session
        self.logger = logger.bind(scraper=self.__class__.__name__)

    @abstractmethod
    def fetch(self) -> list[dict[str, Any]]:
        """
        Fetch injuries from the source.

        Returns:
            List of injury dictionaries with keys:
            - player_name: str
            - team: str (team abbreviation or name)
            - status: str (e.g., "Out", "Day-to-Day")
            - injury_type: str | None
            - body_part: str | None
            - injury_date: date | None
            - notes: str

        Raises:
            Exception: If fetch fails.
        """
        pass

    def parse_injury_description(self, desc: str | None) -> tuple[str | None, str | None]:
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
            "acl",
            "mcl",
            "pcl",
            "lcl",
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
            "illness",
            "infection",
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

    def parse_date(self, date_str: str | None) -> date | None:
        """
        Parse date string into date object.

        Tries multiple common date formats. Handles variations across sources.

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

        self.logger.debug("Failed to parse date", date_str=date_str)
        return None

    def normalize_team_name(self, team: str | None) -> str:
        """
        Normalize team name to standard format.

        Removes extra whitespace and standardizes abbreviations.

        Args:
            team: Raw team name or abbreviation.

        Returns:
            Normalized team name.
        """
        if not team:
            return ""

        # Remove extra whitespace
        normalized = " ".join(team.split())

        # Common team name mappings (if needed)
        # This can be expanded based on actual data

        return normalized


class ESPNInjuryScraper(BaseInjuryScraper):
    """
    Scraper for ESPN NBA injuries page.

    Fetches injury data from https://www.espn.com/nba/injuries
    ESPN provides structured tables with player, team, status, and description.
    """

    BASE_URL = "https://www.espn.com/nba/injuries"

    def fetch(self) -> list[dict[str, Any]]:
        """
        Fetch injuries from ESPN NBA injuries page.

        Returns:
            List of injury dictionaries.

        Raises:
            Exception: If fetch fails.
        """
        url = self.BASE_URL

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
                        team = self.normalize_team_name(team)

                        # Extract status
                        status_cell = cols[2] if len(cols) > 2 else None
                        status = status_cell.get_text(strip=True) if status_cell else ""

                        # Extract injury description
                        desc_cell = cols[3] if len(cols) > 3 else None
                        injury_desc = desc_cell.get_text(strip=True) if desc_cell else ""

                        # Parse injury description for type and body part
                        injury_type, body_part = self.parse_injury_description(injury_desc)

                        # Extract date (if available)
                        date_cell = cols[4] if len(cols) > 4 else None
                        injury_date_str = date_cell.get_text(strip=True) if date_cell else ""
                        injury_date = self.parse_date(injury_date_str)

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


class RotowireInjuryScraper(BaseInjuryScraper):
    """
    Scraper for Rotowire NBA injuries page.

    Fetches injury data from https://www.rotowire.com/basketball/nba-injuries.php
    Rotowire provides detailed injury listings with news and status updates.
    """

    BASE_URL = "https://www.rotowire.com/basketball/nba-injuries.php"

    def fetch(self) -> list[dict[str, Any]]:
        """
        Fetch injuries from Rotowire NBA injuries page.

        Returns:
            List of injury dictionaries.

        Raises:
            Exception: If fetch fails.
        """
        url = self.BASE_URL

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
                team = self.normalize_team_name(team)

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
                    injury_type, body_part = self.parse_injury_description(injury_desc)

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
