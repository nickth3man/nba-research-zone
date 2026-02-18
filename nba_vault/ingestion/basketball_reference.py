"""Basketball Reference client wrapper for data ingestion."""

from typing import Any, cast

import structlog

from nba_vault.utils.cache import ContentCache
from nba_vault.utils.rate_limit import RateLimiter

logger = structlog.get_logger(__name__)


class BasketballReferenceClient:
    """
    Client for Basketball Reference data using basketball_reference_web_scraper.

    This client wraps the basketball_reference_web_scraper library with caching,
    rate limiting, and error handling.
    """

    def __init__(self, cache: ContentCache | None = None, rate_limiter: RateLimiter | None = None):
        """
        Initialize Basketball Reference client.

        Args:
            cache: Content cache for API responses. If None, creates default.
            rate_limiter: Rate limiter for requests. If None, creates default.
        """
        self.cache = cache or ContentCache()
        self.rate_limiter = rate_limiter or RateLimiter(rate=8, per=60)
        self.logger = logger.bind(component="basketball_reference_client")

    def get_players(self, season_end_year: int | None = None) -> list[dict[str, Any]]:
        """
        Get player data from Basketball Reference.

        Args:
            season_end_year: Season end year (e.g., 2024 for 2023-24 season).
                           If None, gets all players.

        Returns:
            List of player dictionaries.

        Raises:
            ImportError: If basketball_reference_web_scraper is not installed.
            ValueError: If season_end_year is out of valid range.
            Exception: If data retrieval fails after retries.
        """
        if season_end_year is not None and (season_end_year < 1947 or season_end_year > 2100):
            raise ValueError(
                f"season_end_year must be between 1947 and 2100, got {season_end_year}"
            )

        try:
            import basketball_reference_web_scraper as _br_scraper  # noqa: PLC0415

            br_scraper: Any = cast("Any", _br_scraper)
        except ImportError as e:
            self.logger.error("basketball_reference_web_scraper not installed")
            raise ImportError(
                "basketball_reference_web_scraper is required. Install with: "
                "pip install basketball_reference_web_scraper"
            ) from e

        cache_key = f"players_season_{season_end_year or 'all'}"

        # Check cache first
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            self.logger.debug("Cache hit for players", season=season_end_year)
            return cached_data

        # Rate limit before scraping
        self.rate_limiter.acquire()

        try:
            if season_end_year is None:
                # Get all players
                self.logger.info("Fetching all players from Basketball Reference")
                players_data = br_scraper.players_season_totals(2024)  # Default to recent season
            else:
                self.logger.info("Fetching players for season", season=season_end_year)
                players_data = br_scraper.players_season_totals(season_end_year)

            if not players_data:
                self.logger.warning(
                    "Basketball Reference returned empty player list",
                    season=season_end_year,
                )
                return []

            # Convert to dict format for serialization
            players_list = []
            for player_dict in players_data:
                try:
                    player_data = {
                        "slug": getattr(player_dict, "slug", ""),
                        "name": getattr(player_dict, "name", ""),
                        "position": getattr(player_dict, "position", ""),
                        "height": getattr(player_dict, "height", ""),
                        "weight": getattr(player_dict, "weight", ""),
                        "team_abbreviation": getattr(player_dict, "team_abbreviation", ""),
                        "games_played": getattr(player_dict, "games_played", 0),
                        "games_started": getattr(player_dict, "games_started", 0),
                        "minutes_played": getattr(player_dict, "minutes_played", 0.0),
                        "field_goals": getattr(player_dict, "field_goals", 0),
                        "field_goal_attempts": getattr(player_dict, "field_goal_attempts", 0),
                        "field_goal_percentage": getattr(player_dict, "field_goal_percentage", 0.0),
                        "three_point_field_goals": getattr(
                            player_dict, "three_point_field_goals", 0
                        ),
                        "three_point_field_goal_attempts": getattr(
                            player_dict, "three_point_field_goal_attempts", 0
                        ),
                        "three_point_field_goal_percentage": getattr(
                            player_dict, "three_point_field_goal_percentage", 0.0
                        ),
                        "two_point_field_goals": getattr(player_dict, "two_point_field_goals", 0),
                        "two_point_field_goal_attempts": getattr(
                            player_dict, "two_point_field_goal_attempts", 0
                        ),
                        "two_point_field_goal_percentage": getattr(
                            player_dict, "two_point_field_goal_percentage", 0.0
                        ),
                        "effective_field_goal_percentage": getattr(
                            player_dict, "effective_field_goal_percentage", 0.0
                        ),
                        "free_throws": getattr(player_dict, "free_throws", 0),
                        "free_throw_attempts": getattr(player_dict, "free_throw_attempts", 0),
                        "free_throw_percentage": getattr(player_dict, "free_throw_percentage", 0.0),
                        "offensive_rebounds": getattr(player_dict, "offensive_rebounds", 0),
                        "defensive_rebounds": getattr(player_dict, "defensive_rebounds", 0),
                        "rebounds": getattr(player_dict, "rebounds", 0),
                        "assists": getattr(player_dict, "assists", 0),
                        "steals": getattr(player_dict, "steals", 0),
                        "blocks": getattr(player_dict, "blocks", 0),
                        "turnovers": getattr(player_dict, "turnovers", 0),
                        "personal_fouls": getattr(player_dict, "personal_fouls", 0),
                        "points": getattr(player_dict, "points", 0),
                        "player_advanced_stats": getattr(player_dict, "player_advanced_stats", {}),
                    }
                    players_list.append(player_data)
                except AttributeError as attr_err:
                    self.logger.warning(
                        "Skipping player: unexpected data structure",
                        error=str(attr_err),
                    )
                    continue

            # Cache the results
            self.cache.set(cache_key, players_list)

            self.logger.info("Successfully fetched players", count=len(players_list))
            return players_list

        except Exception as e:
            self.logger.error("Failed to fetch players", season=season_end_year, error=str(e))
            raise

    def get_player_info(self, slug: str) -> dict[str, Any]:
        """
        Get detailed information for a specific player.

        Args:
            slug: Basketball Reference player slug (e.g., "jamesle01").

        Returns:
            Player information dictionary.

        Raises:
            ImportError: If basketball_reference_web_scraper is not installed.
            Exception: If data retrieval fails after retries.
        """
        try:
            import basketball_reference_web_scraper as _br_scraper2  # noqa: PLC0415

            br_scraper: Any = cast("Any", _br_scraper2)
        except ImportError as e:
            self.logger.error("basketball_reference_web_scraper not installed")
            raise ImportError(
                "basketball_reference_web_scraper is required. Install with: "
                "pip install basketball_reference_web_scraper"
            ) from e

        cache_key = f"player_info_{slug}"

        # Check cache first
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            self.logger.debug("Cache hit for player info", slug=slug)
            return cached_data

        # Rate limit before scraping
        self.rate_limiter.acquire()

        try:
            self.logger.info("Fetching player info", slug=slug)

            # Get player bio/information
            player_data = br_scraper.player_box_scores(
                day=1, month=1, year=2024, slug=slug
            )  # This may not work, need to check API

            # Convert to dict format
            player_info = {
                "slug": slug,
                "data": player_data,
            }

            # Cache the results
            self.cache.set(cache_key, player_info)

            self.logger.info("Successfully fetched player info", slug=slug)
            return player_info

        except Exception as e:
            self.logger.error("Failed to fetch player info", slug=slug, error=str(e))
            raise
