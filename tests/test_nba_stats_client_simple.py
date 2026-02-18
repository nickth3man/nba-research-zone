"""Tests for NBAStatsClient.

Tests cover the main API client functionality.
"""

from unittest.mock import Mock, patch

from nba_vault.ingestion.nba_stats_client import NBAStatsClient
from nba_vault.utils.cache import ContentCache
from nba_vault.utils.rate_limit import RateLimiter


class TestNBAStatsClientInit:
    """Tests for NBAStatsClient initialization."""

    def test_init_default_parameters(self):
        """Test initialization with default parameters."""
        client = NBAStatsClient()

        assert client.cache is not None
        assert isinstance(client.cache, ContentCache)
        assert client.rate_limiter is not None
        assert isinstance(client.rate_limiter, RateLimiter)
        assert client.timeout == 30

    def test_init_custom_parameters(self):
        """Test initialization with custom parameters."""
        cache = ContentCache()
        rate_limiter = RateLimiter(rate=10, per=60)

        client = NBAStatsClient(cache=cache, rate_limiter=rate_limiter, timeout=60)

        assert client.cache == cache
        assert client.rate_limiter == rate_limiter
        assert client.timeout == 60


class TestNBAStatsClientAPIEndpoints:
    """Tests for NBAStatsClient API endpoint methods."""

    def test_get_player_tracking(self):
        """Test get_player_tracking endpoint."""
        client = NBAStatsClient()

        with patch.object(client, "_make_request") as mock_request:
            mock_request.return_value = {"PlayerTracking": {"data": []}}

            client.get_player_tracking(
                player_id=2544,
                season="2023-24",
                season_type="Regular Season",
            )

            assert mock_request.called

    def test_get_team_lineups(self):
        """Test get_team_lineups endpoint."""
        client = NBAStatsClient()

        with patch.object(client, "_make_request") as mock_request:
            mock_request.return_value = {"Lineups": {"data": []}}

            client.get_team_lineups(
                team_id=1610612747,
                season="2023-24",
            )

            assert mock_request.called

    def test_get_all_lineups(self):
        """Test get_all_lineups endpoint."""
        client = NBAStatsClient()

        with patch.object(client, "_make_request") as mock_request:
            mock_request.return_value = {"Lineups": {"data": []}}

            client.get_all_lineups(season="2023-24")

            assert mock_request.called

    def test_get_box_score_summary(self):
        """Test get_box_score_summary endpoint."""
        client = NBAStatsClient()

        with patch.object(client, "_make_request") as mock_request:
            mock_request.return_value = {"GameSummary": {"data": []}}

            client.get_box_score_summary(game_id="0022300001")

            assert mock_request.called

    def test_get_team_advanced_stats(self):
        """Test get_team_advanced_stats endpoint."""
        client = NBAStatsClient()

        with patch.object(client, "_make_request") as mock_request:
            mock_request.return_value = {"TeamStats": {"data": []}}

            client.get_team_advanced_stats(
                team_id=1610612747,
                season="2023-24",
            )

            assert mock_request.called


class TestNBAStatsClientStaticMethods:
    """Tests for NBAStatsClient static helper methods."""

    def test_get_team_id_by_abbreviation(self):
        """Test getting team ID by abbreviation."""
        client = NBAStatsClient()
        client.adapter.get_team_id_by_abbreviation = Mock(return_value=1610612747)  # type: ignore[method-assign]

        result = client.get_team_id_by_abbreviation("LAL")
        assert result == 1610612747

    def test_get_team_id_by_abbreviation_case_insensitive(self):
        """Test team ID lookup is case-insensitive (delegated to adapter)."""
        client = NBAStatsClient()
        client.adapter.get_team_id_by_abbreviation = Mock(return_value=1610612747)  # type: ignore[method-assign]

        result = client.get_team_id_by_abbreviation("lal")
        assert result == 1610612747

    def test_get_team_id_by_abbreviation_not_found(self):
        """Test that non-existent team returns None."""
        client = NBAStatsClient()
        client.adapter.get_team_id_by_abbreviation = Mock(return_value=None)  # type: ignore[method-assign]

        result = client.get_team_id_by_abbreviation("XXX")
        assert result is None

    def test_get_team_id_by_abbreviation_error_handling(self):
        """Test error handling in team ID lookup."""
        client = NBAStatsClient()
        client.adapter.get_team_id_by_abbreviation = Mock(return_value=None)  # type: ignore[method-assign]

        result = client.get_team_id_by_abbreviation("LAL")
        assert result is None

    def test_get_player_id_by_name(self):
        """Test getting player ID by name."""
        client = NBAStatsClient()
        client.adapter.get_player_id_by_name = Mock(return_value=2544)  # type: ignore[method-assign]

        result = client.get_player_id_by_name("LeBron James")
        assert result == 2544

    def test_get_player_id_by_name_case_insensitive(self):
        """Test player ID lookup is case-insensitive (delegated to adapter)."""
        client = NBAStatsClient()
        client.adapter.get_player_id_by_name = Mock(return_value=2544)  # type: ignore[method-assign]

        result = client.get_player_id_by_name("lebron james")
        assert result == 2544

    def test_get_player_id_by_name_not_found(self):
        """Test that non-existent player returns None."""
        client = NBAStatsClient()
        client.adapter.get_player_id_by_name = Mock(return_value=None)  # type: ignore[method-assign]

        result = client.get_player_id_by_name("Michael Jordan")
        assert result is None

    def test_get_player_id_by_name_error_handling(self):
        """Test error handling in player ID lookup."""
        client = NBAStatsClient()
        client.adapter.get_player_id_by_name = Mock(return_value=None)  # type: ignore[method-assign]

        result = client.get_player_id_by_name("LeBron James")
        assert result is None
