"""Tests for NBA Stats Adapter pattern.

These tests demonstrate the benefits of the adapter pattern:
- Easy testing with mock adapters
- Swapping implementations without changing client code
- Isolation of library-specific quirks
"""

from typing import Any

from nba_vault.ingestion.adapters import NbaApiAdapter, NBAStatsAdapter


class MockCache:
    """Mock cache that never returns cached values."""

    def get(self, key: str) -> None:
        """Always return None to bypass cache."""
        return None

    def set(self, key: str, value: Any) -> None:
        """Do nothing."""
        pass


class MockNBAStatsAdapter(NBAStatsAdapter):
    """Mock adapter for testing without actual API calls."""

    def __init__(self):
        """Initialize mock adapter with test data."""
        self.call_count = 0
        self.last_call: tuple[str, dict] | None = None

    def get_player_tracking(self, **kwargs):  # type: ignore[override]
        """Return mock player tracking data."""
        self.call_count += 1
        self.last_call = ("get_player_tracking", kwargs)
        return {
            "PlayerTracking": {
                "data": [[2544, "2023-24", 1610612747, 2.5, 4.2]],
                "headers": ["PLAYER_ID", "SEASON", "TEAM_ID", "DIST_MILES", "SPD"],
            }
        }

    def get_team_lineups(self, **kwargs):  # type: ignore[override]
        """Return mock team lineup data."""
        self.call_count += 1
        self.last_call = ("get_team_lineups", kwargs)
        return {
            "Lineups": {
                "data": [[1, 2, 3, 4, 5, 1610612747, 100.0]],
                "headers": [
                    "PLAYER_ID_1",
                    "PLAYER_ID_2",
                    "PLAYER_ID_3",
                    "PLAYER_ID_4",
                    "PLAYER_ID_5",
                    "TEAM_ID",
                    "MIN",
                ],
            }
        }

    def get_all_lineups(self, **kwargs):  # type: ignore[override]
        """Return mock all lineups data."""
        self.call_count += 1
        self.last_call = ("get_all_lineups", kwargs)
        return {
            "Lineups": {
                "data": [[1, 2, 3, 4, 5, 1610612747, 100.0]],
                "headers": [
                    "PLAYER_ID_1",
                    "PLAYER_ID_2",
                    "PLAYER_ID_3",
                    "PLAYER_ID_4",
                    "PLAYER_ID_5",
                    "TEAM_ID",
                    "MIN",
                ],
            }
        }

    def get_box_score_summary(self, **kwargs):  # type: ignore[override]
        """Return mock box score data."""
        self.call_count += 1
        self.last_call = ("get_box_score_summary", kwargs)
        return {
            "GameSummary": {
                "data": [["2023-10-25", "LAL", "GSW", 120, 110]],
                "headers": ["GAME_DATE", "HOME_TEAM", "VISITOR_TEAM", "HOME PTS", "VISITOR PTS"],
            }
        }

    def get_team_advanced_stats(self, **kwargs):  # type: ignore[override]
        """Return mock team advanced stats."""
        self.call_count += 1
        self.last_call = ("get_team_advanced_stats", kwargs)
        return {
            "TeamStats": {
                "data": [[1610612747, 115.2, 110.5, 4.7]],
                "headers": ["TEAM_ID", "OFF_RATING", "DEF_RATING", "NET_RATING"],
            }
        }

    def get_team_id_by_abbreviation(self, abbreviation: str):
        """Return mock team ID."""
        self.call_count += 1
        self.last_call = ("get_team_id_by_abbreviation", {"abbreviation": abbreviation})
        if abbreviation.upper() == "LAL":
            return 1610612747
        return None

    def get_player_id_by_name(self, full_name: str):
        """Return mock player ID."""
        self.call_count += 1
        self.last_call = ("get_player_id_by_name", {"full_name": full_name})
        if full_name.lower() == "lebron james":
            return 2544
        return None


class TestMockAdapter:
    """Tests demonstrating mock adapter usage."""

    def test_mock_adapter_interface(self):
        """Test that mock adapter implements the interface correctly."""
        adapter = MockNBAStatsAdapter()

        # Test get_player_tracking
        result = adapter.get_player_tracking(player_id=2544, season="2023-24")
        assert result["PlayerTracking"]["data"][0][0] == 2544
        assert adapter.call_count == 1

        # Test get_team_lineups
        result = adapter.get_team_lineups(team_id=1610612747, season="2023-24")
        assert result["Lineups"]["data"][0][5] == 1610612747
        assert adapter.call_count == 2

        # Test get_team_id_by_abbreviation
        result = adapter.get_team_id_by_abbreviation("LAL")
        assert result == 1610612747
        assert adapter.call_count == 3

        # Test get_player_id_by_name
        result = adapter.get_player_id_by_name("LeBron James")
        assert result == 2544
        assert adapter.call_count == 4

    def test_client_with_mock_adapter(self):
        """Test NBAStatsClient using mock adapter."""
        from nba_vault.ingestion.nba_stats_client import NBAStatsClient

        # Use mock cache to ensure adapter is called every time
        mock_adapter = MockNBAStatsAdapter()
        client = NBAStatsClient(adapter=mock_adapter, cache=MockCache())  # type: ignore[arg-type]

        # Test get_player_tracking through client
        result = client.get_player_tracking(player_id=2544, season="2023-24")
        assert result["PlayerTracking"]["data"][0][0] == 2544

        # Test get_team_lineups through client
        result = client.get_team_lineups(team_id=1610612747, season="2023-24")
        assert result["Lineups"]["data"][0][5] == 1610612747

        # Test get_team_id_by_abbreviation through client
        result = client.get_team_id_by_abbreviation("LAL")
        assert result == 1610612747

        # Test get_player_id_by_name through client
        result = client.get_player_id_by_name("LeBron James")
        assert result == 2544

        # Verify adapter was called 4 times (cache bypassed)
        assert mock_adapter.call_count == 4

    def test_adapter_swapping(self):
        """Test that we can easily swap adapters."""
        from nba_vault.ingestion.nba_stats_client import NBAStatsClient

        # Create client with mock adapter
        mock_adapter = MockNBAStatsAdapter()
        client = NBAStatsClient(adapter=mock_adapter)

        # Use mock adapter
        result = client.get_team_id_by_abbreviation("LAL")
        assert result == 1610612747

        # Swap to real adapter (will fail if nba_api not installed)
        # This demonstrates the ability to swap implementations
        try:
            real_adapter = NbaApiAdapter(timeout=30)
            client.adapter = real_adapter

            # Now client uses real adapter
            # (we won't call it to avoid actual API requests)
            assert isinstance(client.adapter, NbaApiAdapter)
            assert not isinstance(client.adapter, MockNBAStatsAdapter)
        except ImportError:
            # nba_api not installed, skip this part
            pass

    def test_adapter_call_tracking(self):
        """Test that we can track adapter calls for testing."""
        mock_adapter = MockNBAStatsAdapter()

        # Make multiple calls
        mock_adapter.get_player_tracking(player_id=2544, season="2023-24")
        mock_adapter.get_team_lineups(team_id=1610612747, season="2023-24")

        # Verify call count
        assert mock_adapter.call_count == 2

        # Verify last call details
        assert mock_adapter.last_call is not None
        method, params = mock_adapter.last_call
        assert method == "get_team_lineups"
        assert params["team_id"] == 1610612747
        assert params["season"] == "2023-24"


class TestAdapterInterface:
    """Tests for adapter interface compliance."""

    def test_nba_api_adapter_implements_interface(self):
        """Test that NbaApiAdapter implements the interface."""
        try:
            adapter = NbaApiAdapter(timeout=30)

            # Check that all required methods exist
            assert hasattr(adapter, "get_player_tracking")
            assert hasattr(adapter, "get_team_lineups")
            assert hasattr(adapter, "get_all_lineups")
            assert hasattr(adapter, "get_box_score_summary")
            assert hasattr(adapter, "get_team_advanced_stats")
            assert hasattr(adapter, "get_team_id_by_abbreviation")
            assert hasattr(adapter, "get_player_id_by_name")

            # Check that methods are callable
            assert callable(adapter.get_player_tracking)
            assert callable(adapter.get_team_lineups)
            assert callable(adapter.get_all_lineups)
            assert callable(adapter.get_box_score_summary)
            assert callable(adapter.get_team_advanced_stats)
            assert callable(adapter.get_team_id_by_abbreviation)
            assert callable(adapter.get_player_id_by_name)

        except ImportError:
            # nba_api not installed, skip test
            pass

    def test_adapter_isolation(self):
        """Test that adapter isolates library-specific quirks."""
        mock_adapter = MockNBAStatsAdapter()

        # The adapter handles all nba_api-specific logic
        # The client doesn't need to know about implementation details
        result = mock_adapter.get_player_tracking(player_id=2544, season="2023-24")

        # Response format is standardized by the adapter
        assert "PlayerTracking" in result
        assert "data" in result["PlayerTracking"]
        assert "headers" in result["PlayerTracking"]

        # Client can rely on this format regardless of implementation
        headers = result["PlayerTracking"]["headers"]
        data = result["PlayerTracking"]["data"]
        assert len(headers) == len(data[0])
