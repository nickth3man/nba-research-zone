"""NBA.com Stats API client wrapper for data ingestion.

This client provides access to NBA.com's stats.nba.com API endpoints
using an adapter pattern, with caching, rate limiting, and error handling.

The client uses an adapter interface to decouple from specific implementations,
making it easy to swap the underlying library (nba_api, direct HTTP, etc.)
without changing client code.
"""

from typing import Any

import structlog

from nba_vault.ingestion.adapters import NbaApiAdapter, NBAStatsAdapter
from nba_vault.utils.cache import ContentCache
from nba_vault.utils.rate_limit import RateLimiter

logger = structlog.get_logger(__name__)


class NBAStatsClient:
    """
    Client for NBA.com Stats API using adapter pattern.

    This client wraps NBA.com Stats API access through an adapter interface,
    providing caching, rate limiting, and error handling for reliable data ingestion.

    NBA.com Stats API provides:
    - Player tracking data (speed, distance, touches, drives)
    - Lineup data (combinations, performance metrics)
    - Team advanced stats (offensive/defensive rating, pace, etc.)
    - Box score data (traditional and advanced)
    - Play-by-play data
    - Shot chart data

    Note: NBA.com does not officially document these APIs and may change
    endpoints without notice. This client handles common errors gracefully.

    The adapter pattern allows:
    - Easy swapping of underlying API implementations
    - Testing with mock adapters
    - Isolation of library-specific quirks
    - Future migration to direct HTTP calls if needed
    """

    def __init__(
        self,
        cache: ContentCache | None = None,
        rate_limiter: RateLimiter | None = None,
        timeout: int = 30,
        adapter: NBAStatsAdapter | None = None,
    ):
        """
        Initialize NBA Stats client.

        Args:
            cache: Content cache for API responses. If None, creates default.
            rate_limiter: Rate limiter for requests. If None, creates default.
                         NBA.com allows ~8 requests/minute before throttling.
            timeout: Request timeout in seconds.
            adapter: NBAStatsAdapter instance. If None, creates NbaApiAdapter.
                   Useful for testing with mock adapters or alternative implementations.
        """
        self.cache = cache or ContentCache()
        # NBA.com is aggressive with rate limiting
        self.rate_limiter = rate_limiter or RateLimiter(rate=6, per=60)
        self.timeout = timeout
        self.adapter = adapter or NbaApiAdapter(timeout=timeout)
        self.logger = logger.bind(component="nba_stats_client")

    def _make_request(
        self,
        method_name: str,
        cache_key: str | None = None,
        **params: Any,
    ) -> dict[str, Any]:
        """
        Make a request to NBA.com Stats API through the adapter.

        This method provides caching, rate limiting, and error handling
        for all adapter method calls.

        Args:
            method_name: Name of the adapter method to call.
            cache_key: Optional cache key. If None, generates from params.
            **params: Parameters to pass to the adapter method.

        Returns:
            Response data as dictionary.

        Raises:
            RateLimitError: If API rate limit is exceeded.
            TimeoutError: If request times out.
            ConnectionError: If connection fails.
            Exception: If request fails after retries.
        """
        if cache_key is None:
            # Generate cache key from method and params
            param_str = "_".join(f"{k}={v}" for k, v in sorted(params.items()))
            cache_key = f"nba_stats_{method_name}_{param_str}"

        # Check cache first
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            self.logger.debug("Cache hit", method=method_name, params=params)
            return cached_data

        # Rate limit before request
        self.rate_limiter.acquire()

        try:
            # Get the method from the adapter
            method = getattr(self.adapter, method_name)

            self.logger.info(
                "Making NBA Stats API request",
                method=method_name,
                params=params,
            )

            # Call the adapter method
            result = method(**params)

            if not result:
                self.logger.warning(
                    "NBA Stats API returned empty response",
                    method=method_name,
                    params=params,
                )

            # Cache the results
            self.cache.set(cache_key, result)

            self.logger.info(
                "Successfully fetched data",
                method=method_name,
                datasets=list(result.keys()),
            )

            return result

        except Exception as e:
            # Log and re-raise adapter exceptions
            self.logger.error(
                "NBA Stats API request failed",
                method=method_name,
                params=params,
                error_type=type(e).__name__,
                error=str(e),
            )
            raise

    def get_player_tracking(
        self,
        player_id: int,
        season: str,
        season_type: str = "Regular Season",
        measure_type: str = "Base",
        per_mode: str = "PerGame",
        plus_minus: str = "N",
        pace_adjust: str = "N",
        rank: str = "N",
        outcome: str = "",
        location: str = "",
        month: int = 0,
        season_segment: str = "",
        date_from: str = "",
        date_to: str = "",
        opponent_team_id: int = 0,
        vs_conference: str = "",
        vs_division: str = "",
        game_segment: str = "",
        period: int = 0,
        shot_clock_range: str = "",
        last_n_games: int = 0,
    ) -> dict[str, Any]:
        """
        Get player tracking data (speed, distance, touches, drives).

        Tracking data is available from 2013-14 season onwards.

        Args:
            player_id: NBA player ID.
            season: Season in format "YYYY-YY" (e.g., "2023-24").
            season_type: "Regular Season", "Playoffs", or "Pre Season".
            measure_type: "Base", "Advanced", etc.
            per_mode: "PerGame", "Totals", "Per36", etc.
            Additional filters: see NBA.com API documentation.

        Returns:
            Dictionary with tracking stats.

        Raises:
            Exception: If request fails.
        """
        return self._make_request(
            "get_player_tracking",
            player_id=player_id,
            season=season,
            season_type=season_type,
            measure_type=measure_type,
            per_mode=per_mode,
            plus_minus=plus_minus,
            pace_adjust=pace_adjust,
            rank=rank,
            outcome=outcome,
            location=location,
            month=month,
            season_segment=season_segment,
            date_from=date_from,
            date_to=date_to,
            opponent_team_id=opponent_team_id,
            vs_conference=vs_conference,
            vs_division=vs_division,
            game_segment=game_segment,
            period=period,
            shot_clock_range=shot_clock_range,
            last_n_games=last_n_games,
        )

    def get_team_lineups(
        self,
        team_id: int,
        season: str,
        season_type: str = "Regular Season",
        measure_type: str = "Base",
        per_mode: str = "PerGame",
        plus_minus: str = "N",
        pace_adjust: str = "N",
        rank: str = "N",
        outcome: str = "",
        location: str = "",
        month: int = 0,
        season_segment: str = "",
        date_from: str = "",
        date_to: str = "",
        opponent_team_id: int = 0,
        vs_conference: str = "",
        vs_division: str = "",
        game_segment: str = "",
        period: int = 0,
        shot_clock_range: str = "",
        last_n_games: int = 0,
        group_quantity: int = 5,
    ) -> dict[str, Any]:
        """
        Get lineup data for a team.

        Args:
            team_id: NBA team ID.
            season: Season in format "YYYY-YY" (e.g., "2023-24").
            season_type: "Regular Season", "Playoffs", or "Pre Season".
            group_quantity: Number of players in lineup (default 5).
            Additional filters: see NBA.com API documentation.

        Returns:
            Dictionary with lineup stats.

        Raises:
            Exception: If request fails.
        """
        return self._make_request(
            "get_team_lineups",
            team_id=team_id,
            season=season,
            season_type=season_type,
            measure_type=measure_type,
            per_mode=per_mode,
            plus_minus=plus_minus,
            pace_adjust=pace_adjust,
            rank=rank,
            outcome=outcome,
            location=location,
            month=month,
            season_segment=season_segment,
            date_from=date_from,
            date_to=date_to,
            opponent_team_id=opponent_team_id,
            vs_conference=vs_conference,
            vs_division=vs_division,
            game_segment=game_segment,
            period=period,
            shot_clock_range=shot_clock_range,
            last_n_games=last_n_games,
            group_quantity=group_quantity,
        )

    def get_all_lineups(
        self,
        season: str,
        season_type: str = "Regular Season",
        measure_type: str = "Base",
        per_mode: str = "Totals",
        plus_minus: str = "N",
        pace_adjust: str = "N",
        rank: str = "N",
        outcome: str = "",
        location: str = "",
        month: int = 0,
        season_segment: str = "",
        date_from: str = "",
        date_to: str = "",
        opponent_team_id: int = 0,
        vs_conference: str = "",
        vs_division: str = "",
        game_segment: str = "",
        period: int = 0,
        shot_clock_range: str = "",
        last_n_games: int = 0,
        group_quantity: int = 5,
    ) -> dict[str, Any]:
        """
        Get all lineups across the league.

        Args:
            season: Season in format "YYYY-YY" (e.g., "2023-24").
            season_type: "Regular Season", "Playoffs", or "Pre Season".
            group_quantity: Number of players in lineup (default 5).
            Additional filters: see NBA.com API documentation.

        Returns:
            Dictionary with all lineup stats.

        Raises:
            Exception: If request fails.
        """
        return self._make_request(
            "get_all_lineups",
            league_id="00",  # NBA
            season=season,
            season_type=season_type,
            measure_type=measure_type,
            per_mode=per_mode,
            plus_minus=plus_minus,
            pace_adjust=pace_adjust,
            rank=rank,
            outcome=outcome,
            location=location,
            month=month,
            season_segment=season_segment,
            date_from=date_from,
            date_to=date_to,
            opponent_team_id=opponent_team_id,
            vs_conference=vs_conference,
            vs_division=vs_division,
            game_segment=game_segment,
            period=period,
            shot_clock_range=shot_clock_range,
            last_n_games=last_n_games,
            group_quantity=group_quantity,
        )

    def get_box_score_summary(
        self,
        game_id: str,
    ) -> dict[str, Any]:
        """
        Get box score summary for a game (includes other stats).

        Args:
            game_id: 10-character NBA.com game ID.

        Returns:
            Dictionary with box score summary including:
            - Game info
            - Line scores
            - Other stats (paint points, fast break, etc.)
            - Officials

        Raises:
            Exception: If request fails.
        """
        return self._make_request(
            "get_box_score_summary",
            game_id=game_id,
        )

    def get_team_advanced_stats(
        self,
        team_id: int,
        season: str,
        season_type: str = "Regular Season",
        measure_type: str = "Advanced",
        per_mode: str = "PerGame",
        plus_minus: str = "N",
        pace_adjust: str = "N",
        rank: str = "N",
        outcome: str = "",
        location: str = "",
        month: int = 0,
        season_segment: str = "",
        date_from: str = "",
        date_to: str = "",
        opponent_team_id: int = 0,
        vs_conference: str = "",
        vs_division: str = "",
        game_segment: str = "",
        period: int = 0,
        shot_clock_range: str = "",
        last_n_games: int = 0,
    ) -> dict[str, Any]:
        """
        Get advanced team stats.

        Args:
            team_id: NBA team ID.
            season: Season in format "YYYY-YY" (e.g., "2023-24").
            measure_type: "Base", "Advanced", "Four Factors", etc.
            Additional filters: see NBA.com API documentation.

        Returns:
            Dictionary with team advanced stats.

        Raises:
            Exception: If request fails.
        """
        return self._make_request(
            "get_team_advanced_stats",
            team_id=team_id,
            season=season,
            season_type=season_type,
            measure_type=measure_type,
            per_mode=per_mode,
            plus_minus=plus_minus,
            pace_adjust=pace_adjust,
            rank=rank,
            outcome=outcome,
            location=location,
            month=month,
            season_segment=season_segment,
            date_from=date_from,
            date_to=date_to,
            opponent_team_id=opponent_team_id,
            vs_conference=vs_conference,
            vs_division=vs_division,
            game_segment=game_segment,
            period=period,
            shot_clock_range=shot_clock_range,
            last_n_games=last_n_games,
        )

    def get_team_id_by_abbreviation(self, abbreviation: str) -> int | None:
        """
        Get NBA team ID from abbreviation.

        Args:
            abbreviation: 3-letter team abbreviation (e.g., "LAL").

        Returns:
            Team ID if found, None otherwise.
        """
        return self.adapter.get_team_id_by_abbreviation(abbreviation)

    def get_player_id_by_name(self, full_name: str) -> int | None:
        """
        Get NBA player ID from full name.

        Args:
            full_name: Player's full name (e.g., "LeBron James").

        Returns:
            Player ID if found, None otherwise.
        """
        return self.adapter.get_player_id_by_name(full_name)
