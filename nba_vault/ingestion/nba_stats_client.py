"""NBA.com Stats API client wrapper for data ingestion.

This client provides access to NBA.com's stats.nba.com API endpoints
using the nba_api library, with caching, rate limiting, and error handling.
"""

from datetime import datetime
from typing import Any, Optional

import structlog

from nba_vault.utils.cache import ContentCache
from nba_vault.utils.rate_limit import RateLimiter, retry_with_backoff

logger = structlog.get_logger(__name__)


class NBAStatsClient:
    """
    Client for NBA.com Stats API using nba_api library.

    This client wraps the nba_api library with caching, rate limiting,
    and error handling for reliable data ingestion.

    NBA.com Stats API provides:
    - Player tracking data (speed, distance, touches, drives)
    - Lineup data (combinations, performance metrics)
    - Team advanced stats (offensive/defensive rating, pace, etc.)
    - Box score data (traditional and advanced)
    - Play-by-play data
    - Shot chart data

    Note: NBA.com does not officially document these APIs and may change
    endpoints without notice. This client handles common errors gracefully.
    """

    def __init__(
        self,
        cache: Optional[ContentCache] = None,
        rate_limiter: Optional[RateLimiter] = None,
        timeout: int = 30,
    ):
        """
        Initialize NBA Stats client.

        Args:
            cache: Content cache for API responses. If None, creates default.
            rate_limiter: Rate limiter for requests. If None, creates default.
                         NBA.com allows ~8 requests/minute before throttling.
            timeout: Request timeout in seconds.
        """
        self.cache = cache or ContentCache()
        # NBA.com is aggressive with rate limiting
        self.rate_limiter = rate_limiter or RateLimiter(rate=6, per=60)
        self.timeout = timeout
        self.logger = logger.bind(component="nba_stats_client")

        try:
            from nba_api.stats.endpoints import (
                leaguedashplayerstats,
                leaguedashteamstats,
                playerdashptstats,
                teamdashlineups,
                boxscoresummaryv2,
                leaguedashlineups,
                teamyearoveryearstats,
            )
            from nba_api.stats.static import teams, players

            self.endpoints = {
                "leaguedashplayerstats": leaguedashplayerstats,
                "leaguedashteamstats": leaguedashteamstats,
                "playerdashptstats": playerdashptstats,
                "teamdashlineups": teamdashlineups,
                "boxscoresummaryv2": boxscoresummaryv2,
                "leaguedashlineups": leaguedashlineups,
                "teamyearoveryearstats": teamyearoveryearstats,
            }
            self.static = {"teams": teams, "players": players}
        except ImportError as e:
            self.logger.error("nba_api not installed")
            raise ImportError(
                "nba_api is required. Install with: pip install nba_api"
            ) from e

    def _make_request(
        self,
        endpoint_name: str,
        cache_key: Optional[str] = None,
        **params: Any,
    ) -> dict[str, Any]:
        """
        Make a request to NBA.com Stats API.

        Args:
            endpoint_name: Name of the endpoint (from self.endpoints).
            cache_key: Optional cache key. If None, generates from params.
            **params: Parameters to pass to the endpoint.

        Returns:
            Response data as dictionary.

        Raises:
            Exception: If request fails after retries.
        """
        if cache_key is None:
            # Generate cache key from endpoint and params
            param_str = "_".join(f"{k}={v}" for k, v in sorted(params.items()))
            cache_key = f"nba_stats_{endpoint_name}_{param_str}"

        # Check cache first
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            self.logger.debug("Cache hit", endpoint=endpoint_name, params=params)
            return cached_data

        # Rate limit before request
        self.rate_limiter.acquire()

        try:
            endpoint_class = self.endpoints.get(endpoint_name)
            if endpoint_class is None:
                raise ValueError(f"Unknown endpoint: {endpoint_name}")

            self.logger.info(
                "Making NBA Stats API request",
                endpoint=endpoint_name,
                params=params,
            )

            # Make request with timeout
            response = endpoint_class(**params, timeout=self.timeout)

            # Extract data from response
            # nba_api returns data in data_sets dict
            result = {}
            if hasattr(response, "data_sets"):
                for dataset_name, dataset in response.data_sets.items():
                    # Get data as dict
                    result[dataset_name] = dataset.get_dict()
            elif hasattr(response, "dict"):
                result = response.dict()

            # Cache the results
            self.cache.set(cache_key, result)

            self.logger.info(
                "Successfully fetched data",
                endpoint=endpoint_name,
                datasets=list(result.keys()),
            )

            return result

        except Exception as e:
            self.logger.error(
                "NBA Stats API request failed",
                endpoint=endpoint_name,
                params=params,
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
            "playerdashptstats",
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
            "teamdashlineups",
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
            "leaguedashlineups",
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
            "boxscoresummaryv2",
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
            "leaguedashteamstats",
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
        )

    def get_team_id_by_abbreviation(self, abbreviation: str) -> Optional[int]:
        """
        Get NBA team ID from abbreviation.

        Args:
            abbreviation: 3-letter team abbreviation (e.g., "LAL").

        Returns:
            Team ID if found, None otherwise.
        """
        try:
            teams_list = self.static["teams"].get_teams()
            for team in teams_list:
                if team["abbreviation"] == abbreviation.upper():
                    return int(team["id"])
        except Exception as e:
            self.logger.error("Failed to get team ID", abbreviation=abbreviation, error=str(e))
        return None

    def get_player_id_by_name(self, full_name: str) -> Optional[int]:
        """
        Get NBA player ID from full name.

        Args:
            full_name: Player's full name (e.g., "LeBron James").

        Returns:
            Player ID if found, None otherwise.
        """
        try:
            players_list = self.static["players"].get_players()
            for player in players_list:
                if player["full_name"].lower() == full_name.lower():
                    return int(player["id"])
        except Exception as e:
            self.logger.error("Failed to get player ID", name=full_name, error=str(e))
        return None
