"""Adapter pattern for NBA.com Stats API.

This module provides an abstraction layer over the nba_api library,
making it easy to swap implementations for testing or future changes.
"""

from abc import ABC, abstractmethod
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class RateLimitError(Exception):
    """Raised when the NBA.com API responds with HTTP 429 (Too Many Requests)."""


class NBAStatsAdapter(ABC):
    """
    Abstract base class for NBA.com Stats API adapters.

    This interface defines the contract for all NBA Stats API adapters.
    Implementations can wrap different libraries (nba_api, direct HTTP, etc.)
    while providing a consistent interface to the client.

    The adapter pattern provides several benefits:
    - Decouples client code from specific library implementations
    - Makes testing easier with mock adapters
    - Allows swapping implementations without changing client code
    - Isolates library-specific quirks and error handling
    """

    @abstractmethod
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

        Args:
            player_id: NBA player ID.
            season: Season in format "YYYY-YY" (e.g., "2023-24").
            season_type: "Regular Season", "Playoffs", or "Pre Season".
            measure_type: "Base", "Advanced", etc.
            per_mode: "PerGame", "Totals", "Per36", etc.
            Additional filters: see NBA.com API documentation.

        Returns:
            Dictionary with tracking stats. Expected format:
            {
                "PlayerTracking": {
                    "data": [[row1], [row2], ...],
                    "headers": ["COL1", "COL2", ...]
                }
            }

        Raises:
            RateLimitError: If API rate limit is exceeded.
            TimeoutError: If request times out.
            ConnectionError: If connection fails.
            Exception: For other errors.
        """
        pass

    @abstractmethod
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
            Dictionary with lineup stats. Expected format:
            {
                "Lineups": {
                    "data": [[row1], [row2], ...],
                    "headers": ["COL1", "COL2", ...]
                }
            }

        Raises:
            RateLimitError: If API rate limit is exceeded.
            TimeoutError: If request times out.
            ConnectionError: If connection fails.
            Exception: For other errors.
        """
        pass

    @abstractmethod
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
            Dictionary with all lineup stats. Expected format:
            {
                "Lineups": {
                    "data": [[row1], [row2], ...],
                    "headers": ["COL1", "COL2", ...]
                }
            }

        Raises:
            RateLimitError: If API rate limit is exceeded.
            TimeoutError: If request times out.
            ConnectionError: If connection fails.
            Exception: For other errors.
        """
        pass

    @abstractmethod
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
            RateLimitError: If API rate limit is exceeded.
            TimeoutError: If request times out.
            ConnectionError: If connection fails.
            Exception: For other errors.
        """
        pass

    @abstractmethod
    def get_team_advanced_stats(
        self,
        season: str,
        team_id: int = 0,
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
            Dictionary with team advanced stats. Expected format:
            {
                "TeamStats": {
                    "data": [[row1], [row2], ...],
                    "headers": ["COL1", "COL2", ...]
                }
            }

        Raises:
            RateLimitError: If API rate limit is exceeded.
            TimeoutError: If request times out.
            ConnectionError: If connection fails.
            Exception: For other errors.
        """
        pass

    @abstractmethod
    def get_team_id_by_abbreviation(self, abbreviation: str) -> int | None:
        """
        Get NBA team ID from abbreviation.

        Args:
            abbreviation: 3-letter team abbreviation (e.g., "LAL").

        Returns:
            Team ID if found, None otherwise.
        """
        pass

    @abstractmethod
    def get_player_id_by_name(self, full_name: str) -> int | None:
        """
        Get NBA player ID from full name.

        Args:
            full_name: Player's full name (e.g., "LeBron James").

        Returns:
            Player ID if found, None otherwise.
        """
        pass

    @abstractmethod
    def get_league_game_log(
        self,
        season: str,
        season_type: str = "Regular Season",
        league_id: str = "00",
        direction: str = "DESC",
        player_or_team: str = "T",
    ) -> dict[str, Any]:
        """Get full league game log for a season (LeagueGameLog endpoint)."""
        pass

    @abstractmethod
    def get_box_score_traditional(self, game_id: str) -> dict[str, Any]:
        """Get traditional box score (player + team rows) for a game."""
        pass

    @abstractmethod
    def get_box_score_advanced(self, game_id: str) -> dict[str, Any]:
        """Get advanced box score for a game."""
        pass

    @abstractmethod
    def get_box_score_hustle(self, game_id: str) -> dict[str, Any]:
        """Get hustle box score for a game (available 2015-16+)."""
        pass

    @abstractmethod
    def get_play_by_play(self, game_id: str, end_period: int = 10) -> dict[str, Any]:
        """Get play-by-play events for a game (available 1996-97+)."""
        pass

    @abstractmethod
    def get_shot_chart(
        self,
        game_id: str,
        player_id: int = 0,
        team_id: int = 0,
        season: str = "",
    ) -> dict[str, Any]:
        """Get shot chart data for a game or player/season (available 1996-97+)."""
        pass

    @abstractmethod
    def get_common_player_info(self, player_id: int) -> dict[str, Any]:
        """Get biographical and career-info for a single player."""
        pass

    @abstractmethod
    def get_common_team_roster(self, team_id: int, season: str) -> dict[str, Any]:
        """Get roster + coach data for a team/season."""
        pass

    @abstractmethod
    def get_player_awards(self, player_id: int) -> dict[str, Any]:
        """Get awards list for a player."""
        pass

    @abstractmethod
    def get_player_career_stats(
        self,
        player_id: int,
        per_mode: str = "PerGame",
    ) -> dict[str, Any]:
        """Get career per-season stats for a player."""
        pass

    @abstractmethod
    def get_draft_history(self, league_id: str = "00") -> dict[str, Any]:
        """Get full draft history for the league."""
        pass

    @abstractmethod
    def get_draft_combine_anthro(self, season_year: str) -> dict[str, Any]:
        """Get draft combine anthropometric measurements for a year."""
        pass

    @abstractmethod
    def get_draft_combine_drills(self, season_year: str) -> dict[str, Any]:
        """Get draft combine drill results for a year."""
        pass


class NbaApiAdapter(NBAStatsAdapter):
    """
    Adapter implementation using the nba_api library.

    This adapter wraps the nba_api library, which is an unofficial
    Python wrapper around NBA.com's Stats API. The library is not
    officially documented and may change without notice.

    The adapter handles:
    - Dynamic endpoint loading with fallback paths
    - nba_api-specific response format (data_sets dict)
    - Error detection and translation to standard exceptions
    - Timeout and connection error handling
    - Rate limit detection (HTTP 429)

    All nba_api-specific quirks are isolated within this adapter.
    """

    def __init__(self, timeout: int = 30):
        """
        Initialize the nba_api adapter.

        Args:
            timeout: Request timeout in seconds.

        Raises:
            ImportError: If nba_api is not installed.
        """
        self.timeout = timeout

        try:
            import importlib  # noqa: PLC0415

            def _load_endpoint(module_path: str, class_name: str) -> Any:
                """Load an endpoint class with fallback to direct import."""
                try:
                    mod = importlib.import_module(module_path)
                    return getattr(mod, class_name, None)
                except (ImportError, ModuleNotFoundError):
                    return None

            nba_endpoints = importlib.import_module("nba_api.stats.endpoints")
            nba_static = importlib.import_module("nba_api.stats.static")

            # Dynamic endpoint loading with multiple fallback paths
            # This handles different nba_api versions and installation methods
            self.endpoints: dict[str, Any] = {
                "leaguedashplayerstats": getattr(nba_endpoints, "LeagueDashPlayerStats", None)
                or _load_endpoint(
                    "nba_api.stats.endpoints.leaguedashplayerstats", "LeagueDashPlayerStats"
                ),
                "leaguedashteamstats": getattr(nba_endpoints, "LeagueDashTeamStats", None)
                or _load_endpoint(
                    "nba_api.stats.endpoints.leaguedashteamstats", "LeagueDashTeamStats"
                ),
                "playerdashptstats": getattr(nba_endpoints, "PlayerDashPtStats", None)
                or _load_endpoint("nba_api.stats.endpoints.playerdashptstats", "PlayerDashPtStats"),
                "teamdashlineups": getattr(nba_endpoints, "TeamDashLineups", None)
                or _load_endpoint("nba_api.stats.endpoints.teamdashlineups", "TeamDashLineups"),
                "boxscoresummaryv2": getattr(nba_endpoints, "BoxScoreSummaryV2", None)
                or _load_endpoint("nba_api.stats.endpoints.boxscoresummaryv2", "BoxScoreSummaryV2"),
                "leaguedashlineups": getattr(nba_endpoints, "LeagueDashLineups", None)
                or _load_endpoint("nba_api.stats.endpoints.leaguedashlineups", "LeagueDashLineups"),
                "teamyearoveryearstats": getattr(nba_endpoints, "TeamYearOverYearStats", None)
                or _load_endpoint(
                    "nba_api.stats.endpoints.teamyearoveryearstats", "TeamYearOverYearStats"
                ),
                # New endpoints
                "leaguegamelog": getattr(nba_endpoints, "LeagueGameLog", None)
                or _load_endpoint("nba_api.stats.endpoints.leaguegamelog", "LeagueGameLog"),
                "boxscoretraditionalv2": getattr(nba_endpoints, "BoxScoreTraditionalV2", None)
                or _load_endpoint(
                    "nba_api.stats.endpoints.boxscoretraditionalv2", "BoxScoreTraditionalV2"
                ),
                "boxscoreadvancedv2": getattr(nba_endpoints, "BoxScoreAdvancedV2", None)
                or _load_endpoint(
                    "nba_api.stats.endpoints.boxscoreadvancedv2", "BoxScoreAdvancedV2"
                ),
                "boxscorehustlev2": getattr(nba_endpoints, "BoxScoreHustleV2", None)
                or _load_endpoint("nba_api.stats.endpoints.boxscorehustlev2", "BoxScoreHustleV2"),
                "playbyplayv2": getattr(nba_endpoints, "PlayByPlayV2", None)
                or _load_endpoint("nba_api.stats.endpoints.playbyplayv2", "PlayByPlayV2"),
                "shotchartdetail": getattr(nba_endpoints, "ShotChartDetail", None)
                or _load_endpoint("nba_api.stats.endpoints.shotchartdetail", "ShotChartDetail"),
                "commonplayerinfo": getattr(nba_endpoints, "CommonPlayerInfo", None)
                or _load_endpoint("nba_api.stats.endpoints.commonplayerinfo", "CommonPlayerInfo"),
                "commonteamroster": getattr(nba_endpoints, "CommonTeamRoster", None)
                or _load_endpoint("nba_api.stats.endpoints.commonteamroster", "CommonTeamRoster"),
                "playerawards": getattr(nba_endpoints, "PlayerAwards", None)
                or _load_endpoint("nba_api.stats.endpoints.playerawards", "PlayerAwards"),
                "playercareerstats": getattr(nba_endpoints, "PlayerCareerStats", None)
                or _load_endpoint("nba_api.stats.endpoints.playercareerstats", "PlayerCareerStats"),
                "drafthistory": getattr(nba_endpoints, "DraftHistory", None)
                or _load_endpoint("nba_api.stats.endpoints.drafthistory", "DraftHistory"),
                "draftcombinenonstatmeasures": getattr(
                    nba_endpoints, "DraftCombineNonStatMeasures", None
                )
                or _load_endpoint(
                    "nba_api.stats.endpoints.draftcombinenonstatmeasures",
                    "DraftCombineNonStatMeasures",
                ),
                "draftcombinedrillresults": getattr(nba_endpoints, "DraftCombineDrillResults", None)
                or _load_endpoint(
                    "nba_api.stats.endpoints.draftcombinedrillresults", "DraftCombineDrillResults"
                ),
            }
            self.static: dict[str, Any] = {
                "teams": getattr(nba_static, "teams", None),
                "players": getattr(nba_static, "players", None),
            }
        except ImportError as e:
            raise ImportError("nba_api is required. Install with: pip install nba_api") from e

    def _call_endpoint(
        self,
        endpoint_name: str,
        **params: Any,
    ) -> dict[str, Any]:
        """
        Call an nba_api endpoint and extract response data.

        This method handles nba_api-specific response format and error handling.

        Args:
            endpoint_name: Name of the endpoint (from self.endpoints).
            **params: Parameters to pass to the endpoint.

        Returns:
            Response data as dictionary with dataset names as keys.

        Raises:
            ValueError: If endpoint is not found.
            RateLimitError: If API returns HTTP 429.
            TimeoutError: If request times out.
            ConnectionError: If connection fails.
            Exception: For other errors.
        """
        endpoint_class = self.endpoints.get(endpoint_name)
        if endpoint_class is None:
            raise ValueError(f"Unknown endpoint: {endpoint_name}")

        try:
            # Make request with timeout
            response = endpoint_class(**params, timeout=self.timeout)

            # Extract data from response using get_normalized_dict() which
            # returns {dataset_name: [row_dict, ...]} regardless of nba_api version.
            result: dict[str, Any] = {}
            if hasattr(response, "get_normalized_dict"):
                normalized = response.get_normalized_dict()
                # get_normalized_dict returns {name: [row, ...]} where each row is a dict
                # Wrap into the {name: {headers: [...], data: [...]}} format expected downstream
                for dataset_name, rows in normalized.items():
                    if rows:
                        headers = list(rows[0].keys())
                        data = [list(row.values()) for row in rows]
                    else:
                        headers = []
                        data = []
                    result[dataset_name] = {"headers": headers, "data": data}
            elif hasattr(response, "data_sets"):
                ds = response.data_sets
                if isinstance(ds, dict):
                    for dataset_name, dataset in ds.items():
                        result[dataset_name] = dataset.get_dict()
                elif isinstance(ds, list):
                    for dataset in ds:
                        if hasattr(dataset, "name") and hasattr(dataset, "get_dict"):
                            result[dataset.name] = dataset.get_dict()

            return result

        except Exception as e:
            error_str = str(e)
            error_type = type(e).__name__

            # Detect HTTP 429 rate-limit responses surfaced by nba_api
            if "429" in error_str or "too many requests" in error_str.lower():
                raise RateLimitError(
                    f"NBA.com rate limit exceeded for endpoint '{endpoint_name}'. "
                    "Wait before retrying."
                ) from e

            # Detect timeout errors
            if "timeout" in error_str.lower() or "timed out" in error_str.lower():
                raise TimeoutError(
                    f"Request to '{endpoint_name}' timed out after {self.timeout}s"
                ) from e

            # Detect connection errors
            if "connection" in error_str.lower() or "network" in error_str.lower():
                raise ConnectionError(
                    f"Cannot connect to NBA Stats API for endpoint '{endpoint_name}': {e}"
                ) from e

            # Re-raise other exceptions
            raise Exception(
                f"NBA Stats API request failed for '{endpoint_name}': {error_type}: {error_str}"
            ) from e

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
        """Get player tracking data using nba_api."""
        return self._call_endpoint(
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
        """Get team lineup data using nba_api."""
        return self._call_endpoint(
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
        """Get all league lineups using nba_api.

        Parameter names are mapped to the nba_api-specific names for LeagueDashLineups.
        """
        return self._call_endpoint(
            "leaguedashlineups",
            season=season,
            season_type_all_star=season_type,
            measure_type_detailed_defense=measure_type,
            per_mode_detailed=per_mode,
            plus_minus=plus_minus,
            pace_adjust=pace_adjust,
            rank=rank,
            outcome_nullable=outcome,
            location_nullable=location,
            month=month,
            season_segment_nullable=season_segment,
            date_from_nullable=date_from,
            date_to_nullable=date_to,
            opponent_team_id=opponent_team_id,
            vs_conference_nullable=vs_conference,
            vs_division_nullable=vs_division,
            game_segment_nullable=game_segment,
            period=period,
            shot_clock_range_nullable=shot_clock_range,
            last_n_games=last_n_games,
            group_quantity=group_quantity,
        )

    def get_box_score_summary(
        self,
        game_id: str,
    ) -> dict[str, Any]:
        """Get box score summary using nba_api."""
        return self._call_endpoint(
            "boxscoresummaryv2",
            game_id=game_id,
        )

    def get_team_advanced_stats(
        self,
        season: str,
        team_id: int = 0,
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
        """Get team advanced stats using nba_api.

        Uses LeagueDashTeamStats which returns all teams.
        Parameter names are mapped to the nba_api-specific names.
        """
        return self._call_endpoint(
            "leaguedashteamstats",
            season=season,
            season_type_all_star=season_type,
            measure_type_detailed_defense=measure_type,
            per_mode_detailed=per_mode,
            plus_minus=plus_minus,
            pace_adjust=pace_adjust,
            rank=rank,
            outcome_nullable=outcome,
            location_nullable=location,
            month=month,
            season_segment_nullable=season_segment,
            date_from_nullable=date_from,
            date_to_nullable=date_to,
            opponent_team_id=opponent_team_id,
            vs_conference_nullable=vs_conference,
            vs_division_nullable=vs_division,
            game_segment_nullable=game_segment,
            period=period,
            shot_clock_range_nullable=shot_clock_range,
            last_n_games=last_n_games,
        )

    def get_team_id_by_abbreviation(self, abbreviation: str) -> int | None:
        """Get team ID from abbreviation using nba_api static data."""
        try:
            teams_list = self.static["teams"].get_teams()
            for team in teams_list:
                if team["abbreviation"] == abbreviation.upper():
                    return int(team["id"])
        except Exception:
            # Silently fail - static data may not be available
            logger.debug("Failed to get team ID from static data", abbreviation=abbreviation)
        return None

    def get_player_id_by_name(self, full_name: str) -> int | None:
        """Get player ID from name using nba_api static data."""
        try:
            players_list = self.static["players"].get_players()
            for player in players_list:
                if player["full_name"].lower() == full_name.lower():
                    return int(player["id"])
        except Exception:
            # Silently fail - static data may not be available
            logger.debug("Failed to get player ID from static data", full_name=full_name)
        return None

    def get_league_game_log(
        self,
        season: str,
        season_type: str = "Regular Season",
        league_id: str = "00",
        direction: str = "DESC",
        player_or_team: str = "T",
    ) -> dict[str, Any]:
        """Get full league game log for a season using LeagueGameLog endpoint."""
        return self._call_endpoint(
            "leaguegamelog",
            season=season,
            season_type_all_star=season_type,
            league_id=league_id,
            direction=direction,
            player_or_team_abbreviation=player_or_team,
            date_from_nullable="",
            date_to_nullable="",
            sorter="DATE",
        )

    def get_box_score_traditional(self, game_id: str) -> dict[str, Any]:
        """Get traditional box score for a game using BoxScoreTraditionalV2."""
        return self._call_endpoint(
            "boxscoretraditionalv2",
            game_id=game_id,
            start_period=1,
            end_period=10,
            start_range=0,
            end_range=28800,
            range_type=0,
        )

    def get_box_score_advanced(self, game_id: str) -> dict[str, Any]:
        """Get advanced box score for a game using BoxScoreAdvancedV2."""
        return self._call_endpoint(
            "boxscoreadvancedv2",
            game_id=game_id,
            start_period=1,
            end_period=10,
            start_range=0,
            end_range=28800,
            range_type=0,
        )

    def get_box_score_hustle(self, game_id: str) -> dict[str, Any]:
        """Get hustle box score for a game using BoxScoreHustleV2 (2015-16+)."""
        return self._call_endpoint(
            "boxscorehustlev2",
            game_id=game_id,
        )

    def get_play_by_play(self, game_id: str, end_period: int = 10) -> dict[str, Any]:
        """Get play-by-play events for a game using PlayByPlayV2 (1996-97+)."""
        return self._call_endpoint(
            "playbyplayv2",
            game_id=game_id,
            start_period=1,
            end_period=end_period,
        )

    def get_shot_chart(
        self,
        game_id: str,
        player_id: int = 0,
        team_id: int = 0,
        season: str = "",
    ) -> dict[str, Any]:
        """Get shot chart data using ShotChartDetail (1996-97+)."""
        return self._call_endpoint(
            "shotchartdetail",
            game_id=game_id,
            player_id=player_id,
            team_id=team_id,
            season_nullable=season,
            league_id="00",
            season_type_all_star="Regular Season",
            context_measure_simple="FGA",
        )

    def get_common_player_info(self, player_id: int) -> dict[str, Any]:
        """Get biographical info for a player using CommonPlayerInfo."""
        return self._call_endpoint(
            "commonplayerinfo",
            player_id=player_id,
            league_id_nullable="",
        )

    def get_common_team_roster(self, team_id: int, season: str) -> dict[str, Any]:
        """Get roster + coach list for a team/season using CommonTeamRoster."""
        return self._call_endpoint(
            "commonteamroster",
            team_id=team_id,
            season=season,
            league_id_nullable="",
        )

    def get_player_awards(self, player_id: int) -> dict[str, Any]:
        """Get player awards using PlayerAwards endpoint."""
        return self._call_endpoint(
            "playerawards",
            player_id=player_id,
        )

    def get_player_career_stats(
        self,
        player_id: int,
        per_mode: str = "PerGame",
    ) -> dict[str, Any]:
        """Get career per-season stats using PlayerCareerStats endpoint."""
        return self._call_endpoint(
            "playercareerstats",
            player_id=player_id,
            per_mode_simple=per_mode,
            league_id_nullable="",
        )

    def get_draft_history(self, league_id: str = "00") -> dict[str, Any]:
        """Get full draft history using DraftHistory endpoint."""
        return self._call_endpoint(
            "drafthistory",
            league_id=league_id,
            season_year_nullable="",
            round_num_nullable="",
            round_pick_nullable="",
            overall_pick_nullable="",
            team_id_nullable="",
            player_id_nullable="",
            top_x_nullable="",
        )

    def get_draft_combine_anthro(self, season_year: str) -> dict[str, Any]:
        """Get draft combine anthropometric data using DraftCombineNonStatMeasures."""
        return self._call_endpoint(
            "draftcombinenonstatmeasures",
            league_id="00",
            season_year=season_year,
        )

    def get_draft_combine_drills(self, season_year: str) -> dict[str, Any]:
        """Get draft combine drill results using DraftCombineDrillResults."""
        return self._call_endpoint(
            "draftcombinedrillresults",
            league_id="00",
            season_year=season_year,
        )
