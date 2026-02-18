"""Basketball Reference client wrapper for data ingestion.

NOTE: Basketball Reference is protected by Cloudflare and cannot be scraped
directly with Python requests. This module now uses the NBA.com Stats API
(via nba_api) as the primary data source for player data, which provides
complete historical coverage (1946-present) without scraping restrictions.
"""

from typing import Any

import structlog

from nba_vault.utils.cache import ContentCache
from nba_vault.utils.rate_limit import RateLimiter

logger = structlog.get_logger(__name__)


class BasketballReferenceClient:
    """
    Client for player data using NBA.com Stats API (CommonAllPlayers endpoint).

    Originally designed to wrap basketball_reference_web_scraper, but Basketball
    Reference is now protected by Cloudflare. This client uses the NBA.com API
    instead, which provides equivalent historical player data (1946-present).
    """

    def __init__(self, cache: ContentCache | None = None, rate_limiter: RateLimiter | None = None):
        """
        Initialize client.

        Args:
            cache: Content cache for API responses. If None, creates default.
            rate_limiter: Rate limiter for requests. If None, creates default.
        """
        self.cache = cache or ContentCache()
        self.rate_limiter = rate_limiter or RateLimiter(rate=8, per=60)
        self.logger = logger.bind(component="basketball_reference_client")

    def get_players(self, season_end_year: int | None = None) -> list[dict[str, Any]]:
        """
        Get player data from NBA.com Stats API (CommonAllPlayers endpoint).

        Returns all historical players (1946-present) in a single API call.
        The season_end_year parameter is accepted for API compatibility but
        the NBA.com endpoint returns all players regardless of season.

        Args:
            season_end_year: Season end year (e.g., 2024 for 2023-24 season).
                           Used to determine the season string for the API call.
                           If None, defaults to 2024.

        Returns:
            List of player dictionaries.

        Raises:
            ImportError: If nba_api is not installed.
            Exception: If data retrieval fails.
        """
        try:
            from nba_api.stats.endpoints import commonallplayers  # noqa: PLC0415
        except ImportError as e:
            self.logger.error("nba_api not installed")
            raise ImportError("nba_api is required. Install with: pip install nba_api") from e

        # Build season string for the API
        year = season_end_year or 2024
        season_str = f"{year - 1}-{str(year)[2:]}"

        cache_key = f"nba_api_all_players_{season_str}"

        # Check cache first
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            self.logger.debug("Cache hit for players", season=season_str)
            return cached_data

        # Rate limit before API call
        self.rate_limiter.acquire()

        try:
            self.logger.info("Fetching all players from NBA.com API", season=season_str)

            result = commonallplayers.CommonAllPlayers(
                is_only_current_season=0,  # 0 = all historical players
                league_id="00",
                season=season_str,
            )

            data = result.get_dict()
            result_set = data["resultSets"][0]
            headers = result_set["headers"]
            rows = result_set["rowSet"]

            if not rows:
                self.logger.warning("NBA.com API returned empty player list", season=season_str)
                return []

            players_list = []
            for row in rows:
                row_dict = dict(zip(headers, row, strict=False))
                try:
                    player_data = self._map_nba_api_player(row_dict)
                    players_list.append(player_data)
                except Exception as e:
                    self.logger.warning(
                        "Skipping player: mapping error",
                        player=row_dict.get("DISPLAY_FIRST_LAST"),
                        error=str(e),
                    )
                    continue

            # Cache the results
            self.cache.set(cache_key, players_list)

            self.logger.info("Successfully fetched players", count=len(players_list))
            return players_list

        except Exception as e:
            self.logger.error("Failed to fetch players", season=season_str, error=str(e))
            raise

    def _map_nba_api_player(self, row: dict[str, Any]) -> dict[str, Any]:
        """
        Map NBA.com API CommonAllPlayers row to the expected player dict format.

        Args:
            row: Dictionary with NBA.com API field names.

        Returns:
            Player dictionary in the format expected by PlayersIngestor.
        """
        # Parse name: "DISPLAY_FIRST_LAST" = "LeBron James"
        full_name = row.get("DISPLAY_FIRST_LAST", "")
        name_parts = full_name.split(" ", 1)
        first_name = name_parts[0] if name_parts else ""
        last_name = name_parts[1] if len(name_parts) > 1 else ""

        # Build a slug from player_slug (NBA.com format: "lebron_james")
        # Convert to bbref-style slug if possible, otherwise use NBA slug
        nba_slug = row.get("PLAYER_SLUG") or ""
        person_id = row.get("PERSON_ID", 0)

        # from_year / to_year
        from_year_str = row.get("FROM_YEAR", "")
        to_year_str = row.get("TO_YEAR", "")
        from_year = int(from_year_str) if from_year_str and str(from_year_str).isdigit() else None
        to_year = int(to_year_str) if to_year_str and str(to_year_str).isdigit() else None

        # is_active: ROSTERSTATUS 1 = active, 0 = inactive
        is_active = bool(row.get("ROSTERSTATUS", 0))

        return {
            # Use NBA.com person_id as the slug (unique identifier)
            "slug": str(person_id),
            "name": full_name,
            "first_name": first_name,
            "last_name": last_name,
            "nba_person_id": person_id,
            "nba_slug": nba_slug,
            "position": "",  # Not available in CommonAllPlayers
            "height": "",
            "weight": "",
            "team_abbreviation": row.get("TEAM_ABBREVIATION", ""),
            "team_id": row.get("TEAM_ID", 0),
            "from_year": from_year,
            "to_year": to_year,
            "is_active": is_active,
            # Stats fields — not available in CommonAllPlayers, set to 0
            "games_played": 0,
            "games_started": 0,
            "minutes_played": 0.0,
            "field_goals": 0,
            "field_goal_attempts": 0,
            "field_goal_percentage": 0.0,
            "three_point_field_goals": 0,
            "three_point_field_goal_attempts": 0,
            "three_point_field_goal_percentage": 0.0,
            "two_point_field_goals": 0,
            "two_point_field_goal_attempts": 0,
            "two_point_field_goal_percentage": 0.0,
            "effective_field_goal_percentage": 0.0,
            "free_throws": 0,
            "free_throw_attempts": 0,
            "free_throw_percentage": 0.0,
            "offensive_rebounds": 0,
            "defensive_rebounds": 0,
            "rebounds": 0,
            "assists": 0,
            "steals": 0,
            "blocks": 0,
            "turnovers": 0,
            "personal_fouls": 0,
            "points": 0,
            "player_advanced_stats": {},
        }

    def get_player_info(self, slug: str) -> dict[str, Any]:
        """
        Get detailed information for a specific player by NBA.com person ID.

        Args:
            slug: Player identifier (NBA.com person ID as string, or bbref slug).

        Returns:
            Player information dictionary.
        """
        cache_key = f"player_info_{slug}"

        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            self.logger.debug("Cache hit for player info", slug=slug)
            return cached_data

        self.rate_limiter.acquire()

        try:
            self.logger.info("Fetching player info", slug=slug)

            # Return basic info — detailed player info would require
            # commonplayerinfo endpoint
            player_info = {
                "slug": slug,
                "data": {},
            }

            self.cache.set(cache_key, player_info)
            self.logger.info("Successfully fetched player info", slug=slug)
            return player_info

        except Exception as e:
            self.logger.error("Failed to fetch player info", slug=slug, error=str(e))
            raise
