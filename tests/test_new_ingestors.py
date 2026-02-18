"""Tests for new data ingestors."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import date

from nba_vault.ingestion.nba_stats_client import NBAStatsClient
from nba_vault.ingestion.player_tracking import PlayerTrackingIngestor
from nba_vault.ingestion.lineups import LineupsIngestor, generate_lineup_id
from nba_vault.ingestion.team_other_stats import TeamOtherStatsIngestor
from nba_vault.ingestion.team_advanced_stats import TeamAdvancedStatsIngestor
from nba_vault.ingestion.injuries import InjuryIngestor
from nba_vault.ingestion.contracts import ContractIngestor
from nba_vault.models.advanced_stats import (
    PlayerGameTrackingCreate,
    LineupCreate,
    TeamGameOtherStatsCreate,
    TeamSeasonAdvancedCreate,
    InjuryCreate,
    PlayerContractCreate,
)


class TestNBAStatsClient:
    """Tests for NBAStatsClient."""

    def test_init(self):
        """Test client initialization."""
        client = NBAStatsClient()
        assert client.cache is not None
        assert client.rate_limiter is not None
        assert client.timeout == 30

    def test_init_custom_params(self):
        """Test client initialization with custom parameters."""
        from nba_vault.utils.cache import ContentCache
        from nba_vault.utils.rate_limit import RateLimiter

        cache = ContentCache()
        rate_limiter = RateLimiter(rate=10, per=60)
        client = NBAStatsClient(cache=cache, rate_limiter=rate_limiter, timeout=60)

        assert client.cache == cache
        assert client.rate_limiter == rate_limiter
        assert client.timeout == 60

    @patch("nba_vault.ingestion.nba_stats_client.NBAStatsClient._make_request")
    def test_get_player_tracking(self, mock_request):
        """Test fetching player tracking data."""
        mock_request.return_value = {
            "PlayerTracking": {
                "data": [[1, "2023-24", 100, 2.5, 1.2, 1.3]],
                "headers": ["PLAYER_ID", "SEASON", "TEAM_ID", "DIST_MILES", "SPD", "MIN"]
            }
        }

        client = NBAStatsClient()
        result = client.get_player_tracking(player_id=1, season="2023-24")

        assert mock_request.called
        assert "PlayerTracking" in result

    @patch("nba_vault.ingestion.nba_stats_client.NBAStatsClient._make_request")
    def test_get_team_lineups(self, mock_request):
        """Test fetching team lineup data."""
        mock_request.return_value = {
            "Lineups": {
                "data": [[1, 2, 3, 4, 5, 100, 50, 100]],
                "headers": ["PLAYER_ID_1", "PLAYER_ID_2", "PLAYER_ID_3", "PLAYER_ID_4", "PLAYER_ID_5", "TEAM_ID", "MIN", "PTS"]
            }
        }

        client = NBAStatsClient()
        result = client.get_team_lineups(team_id=100, season="2023-24")

        assert mock_request.called
        assert "Lineups" in result


class TestPlayerTrackingIngestor:
    """Tests for PlayerTrackingIngestor."""

    def test_init(self):
        """Test ingestor initialization."""
        ingestor = PlayerTrackingIngestor()
        assert ingestor.entity_type == "player_tracking"
        assert ingestor.nba_client is not None

    def test_fetch_invalid_season(self):
        """Test that fetching pre-2013 season raises error."""
        ingestor = PlayerTrackingIngestor()
        with pytest.raises(ValueError, match="only available from 2013-14"):
            ingestor.fetch("123", season="2010-11")

    @patch("nba_vault.ingestion.nba_stats_client.NBAStatsClient.get_player_tracking")
    def test_fetch_single_player(self, mock_get_tracking):
        """Test fetching tracking data for single player."""
        mock_get_tracking.return_value = {
            "PlayerTracking": {
                "data": [[1, "2023-24", 100, 2.5, 1.2, 1.3]],
                "headers": ["PLAYER_ID", "SEASON", "TEAM_ID", "DIST_MILES", "SPD", "MIN"]
            }
        }

        ingestor = PlayerTrackingIngestor()
        result = ingestor.fetch("2544", season="2023-24")

        assert result["player_id"] == 2544
        assert result["season"] == "2023-24"
        assert "data" in result

    def test_validate_empty_data(self):
        """Test validating empty data returns empty list."""
        ingestor = PlayerTrackingIngestor()
        result = ingestor.validate({"data": {}, "player_id": 1, "season": "2023-24"})
        assert result == []

    def test_validate_with_data(self):
        """Test validating tracking data."""
        ingestor = PlayerTrackingIngestor()
        raw_data = {
            "data": {
                "PlayerTracking": {
                    "data": [[2544, "2023-24", 1610612747, 2.5, 4.2, 1.2, 1.3, 50, 10, 5, 2, 20, 15, 15, 10]],
                    "headers": [
                        "PLAYER_ID", "SEASON", "TEAM_ID", "DIST_MILES", "DIST_MILES_OFF",
                        "DIST_MILES_DEF", "SPD", "TOUCHES", "EFC", "PAINT", "POST",
                        "DRIVES", "DRIVES_PTS", "PULL_UP_FGA", "PULL_UP_FGM"
                    ]
                }
            },
            "player_id": 2544,
            "season": "2023-24"
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 1
        assert isinstance(result[0], PlayerGameTrackingCreate)
        assert result[0].player_id == 2544

    def test_safe_float_conversion(self):
        """Test safe float conversion."""
        assert PlayerTrackingIngestor._safe_float("2.5") == 2.5
        assert PlayerTrackingIngestor._safe_float("") is None
        assert PlayerTrackingIngestor._safe_float(None) is None
        assert PlayerTrackingIngestor._safe_float("-") is None

    def test_safe_int_conversion(self):
        """Test safe int conversion."""
        assert PlayerTrackingIngestor._safe_int("10") == 10
        assert PlayerTrackingIngestor._safe_int("10.5") == 10
        assert PlayerTrackingIngestor._safe_int("") is None
        assert PlayerTrackingIngestor._safe_int(None) is None


class TestLineupsIngestor:
    """Tests for LineupsIngestor."""

    def test_init(self):
        """Test ingestor initialization."""
        ingestor = LineupsIngestor()
        assert ingestor.entity_type == "lineups"
        assert ingestor.nba_client is not None

    def test_generate_lineup_id(self):
        """Test lineup ID generation."""
        id1 = generate_lineup_id(1, 2, 3, 4, 5)
        id2 = generate_lineup_id(5, 4, 3, 2, 1)  # Same players, different order
        assert id1 == id2  # Should be same regardless of order

        id3 = generate_lineup_id(1, 2, 3, 4, 6)  # Different player
        assert id1 != id3

    @patch("nba_vault.ingestion.nba_stats_client.NBAStatsClient.get_all_lineups")
    def test_fetch_league_lineups(self, mock_get_lineups):
        """Test fetching all lineups in league."""
        mock_get_lineups.return_value = {
            "Lineups": {
                "data": [[1, 2, 3, 4, 5, 100, 50, 100]],
                "headers": ["PLAYER_ID_1", "PLAYER_ID_2", "PLAYER_ID_3", "PLAYER_ID_4", "PLAYER_ID_5", "TEAM_ID", "MIN", "PTS"]
            }
        }

        ingestor = LineupsIngestor()
        result = ingestor.fetch("league", season="2023-24")

        assert result["scope"] == "league"
        assert "data" in result

    def test_validate_with_data(self):
        """Test validating lineup data."""
        ingestor = LineupsIngestor()
        raw_data = {
            "data": {
                "Lineups": {
                    "data": [[1610612747, 1, 2, 3, 4, 5, 100.0, 50, 100, 95, 110.5, 105.2, 5.3]],
                    "headers": [
                        "TEAM_ID", "PLAYER_ID_1", "PLAYER_ID_2", "PLAYER_ID_3",
                        "PLAYER_ID_4", "PLAYER_ID_5", "MIN", "POSS", "PTS",
                        "PTS_ALLOWED", "OFF_RATING", "DEF_RATING", "NET_RATING"
                    ]
                }
            },
            "scope": "league",
            "season": "2023-24"
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 1
        assert isinstance(result[0], LineupCreate)
        assert result[0].team_id == 1610612747


class TestTeamOtherStatsIngestor:
    """Tests for TeamOtherStatsIngestor."""

    def test_init(self):
        """Test ingestor initialization."""
        ingestor = TeamOtherStatsIngestor()
        assert ingestor.entity_type == "team_other_stats"
        assert ingestor.nba_client is not None

    @patch("nba_vault.ingestion.nba_stats_client.NBAStatsClient.get_box_score_summary")
    def test_fetch_game_stats(self, mock_get_boxscore):
        """Test fetching other stats for a game."""
        mock_get_boxscore.return_value = {
            "OtherStats": {
                "data": [[1610612747, 50, 20, 15, 10, 5, 3, 8, 15, 20]],
                "headers": [
                    "TEAM_ID", "PTS_PAINT", "PTS_2ND_CHANCE", "PTS_FB",
                    "LARGEST_LEAD", "LEAD_CHANGES", "TIMES_TIED",
                    "TEAM_TO", "TEAM_REB", "PTS_OFF_TO"
                ]
            }
        }

        ingestor = TeamOtherStatsIngestor()
        result = ingestor.fetch("0022300001", season="2023-24")

        assert result["scope"] == "game"
        assert result["game_id"] == "0022300001"

    def test_safe_int_conversion(self):
        """Test safe int conversion."""
        assert TeamOtherStatsIngestor._safe_int("10") == 10
        assert TeamOtherStatsIngestor._safe_int("") is None
        assert TeamOtherStatsIngestor._safe_int(None) is None
        assert TeamOtherStatsIngestor._safe_int("-") is None


class TestTeamAdvancedStatsIngestor:
    """Tests for TeamAdvancedStatsIngestor."""

    def test_init(self):
        """Test ingestor initialization."""
        ingestor = TeamAdvancedStatsIngestor()
        assert ingestor.entity_type == "team_advanced_stats"
        assert ingestor.nba_client is not None

    @patch("nba_vault.ingestion.nba_stats_client.NBAStatsClient.get_team_advanced_stats")
    def test_fetch_league_stats(self, mock_get_stats):
        """Test fetching advanced stats for all teams."""
        mock_get_stats.return_value = {
            "TeamStats": {
                "data": [[1610612747, 115.2, 110.5, 4.7, 100.5, 0.520, 12.3, 25.4, 0.25, 0.35, 0.580]],
                "headers": [
                    "TEAM_ID", "OFF_RATING", "DEF_RATING", "NET_RATING", "PACE",
                    "EFG_PCT", "TM_TOV_PCT", "OREB_PCT", "FT_RATE", "FG3A_RATE", "TS_PCT"
                ]
            }
        }

        ingestor = TeamAdvancedStatsIngestor()
        result = ingestor.fetch("league", season="2023-24")

        assert result["scope"] == "league"
        assert "data" in result

    def test_validate_with_data(self):
        """Test validating advanced stats data."""
        ingestor = TeamAdvancedStatsIngestor()
        raw_data = {
            "data": {
                "TeamStats": {
                    "data": [[1610612747, 115.2, 110.5, 4.7, 100.5, 0.520, 12.3, 25.4, 0.25, 0.35, 0.580]],
                    "headers": [
                        "TEAM_ID", "OFF_RATING", "DEF_RATING", "NET_RATING", "PACE",
                        "EFG_PCT", "TM_TOV_PCT", "OREB_PCT", "FTA_RATE", "FG3A_RATE", "TS_PCT"
                    ]
                }
            },
            "scope": "league",
            "season": "2023-24"
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 1
        assert isinstance(result[0], TeamSeasonAdvancedCreate)
        assert result[0].team_id == 1610612747
        assert result[0].off_rating == 115.2


class TestInjuryIngestor:
    """Tests for InjuryIngestor."""

    def test_init(self):
        """Test ingestor initialization."""
        ingestor = InjuryIngestor()
        assert ingestor.entity_type == "injuries"
        assert ingestor.session is not None

    def test_fetch_unsupported_source(self):
        """Test that unsupported source raises error."""
        ingestor = InjuryIngestor()
        with pytest.raises(ValueError, match="Unsupported source"):
            ingestor.fetch("all", source="invalid")

    def test_parse_injury_description(self):
        """Test parsing injury descriptions."""
        ingestor = InjuryIngestor()

        injury_type, body_part = ingestor._parse_injury_description("Left ACL tear")
        assert injury_type == "tear"
        assert body_part == "acl"

        injury_type, body_part = ingestor._parse_injury_description("Right ankle sprain")
        assert injury_type == "sprain"
        assert body_part == "ankle"

        injury_type, body_part = ingestor._parse_injury_description("")
        assert injury_type is None
        assert body_part is None

    def test_parse_date(self):
        """Test date parsing."""
        ingestor = InjuryIngestor()

        result = ingestor._parse_date("2023-10-15")
        assert result == date(2023, 10, 15)

        result = ingestor._parse_date("10/15/2023")
        assert result == date(2023, 10, 15)

        result = ingestor._parse_date("")
        assert result is None


class TestContractIngestor:
    """Tests for ContractIngestor."""

    def test_init(self):
        """Test ingestor initialization."""
        ingestor = ContractIngestor()
        assert ingestor.entity_type == "contracts"
        assert ingestor.session is not None

    def test_fetch_unsupported_source(self):
        """Test that unsupported source raises error."""
        ingestor = ContractIngestor()
        with pytest.raises(ValueError, match="Unsupported source"):
            ingestor.fetch("all", source="invalid")

    def test_validate_with_data(self):
        """Test validating contract data."""
        ingestor = ContractIngestor()
        raw_data = {
            "contracts": [
                {
                    "player_id": 2544,
                    "team_id": 1610612747,
                    "season_start": 2023,
                    "season_end": 2026,
                    "salary_amount": 50000000.0,
                    "contract_type": "Veteran",
                    "player_option": 1,
                    "team_option": 0,
                    "early_termination": 0,
                    "guaranteed_money": 50000000.0,
                }
            ]
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 1
        assert isinstance(result[0], PlayerContractCreate)
        assert result[0].player_id == 2544
        assert result[0].salary_amount == 50000000.0


class TestIngestorRegistry:
    """Tests for ingestor registry."""

    def test_all_new_ingestors_registered(self):
        """Test that all new ingestors are registered."""
        from nba_vault.ingestion import get_ingestor, list_ingestors

        # Check that all new ingestors are in the registry
        registered_types = list_ingestors()

        assert "player_tracking" in registered_types
        assert "lineups" in registered_types
        assert "team_other_stats" in registered_types
        assert "team_advanced_stats" in registered_types
        assert "injuries" in registered_types
        assert "contracts" in registered_types

    def test_create_ingestor_instances(self):
        """Test creating ingestor instances."""
        from nba_vault.ingestion import create_ingestor

        player_tracking = create_ingestor("player_tracking")
        assert player_tracking is not None
        assert isinstance(player_tracking, PlayerTrackingIngestor)

        lineups = create_ingestor("lineups")
        assert lineups is not None
        assert isinstance(lineups, LineupsIngestor)

        team_other = create_ingestor("team_other_stats")
        assert team_other is not None
        assert isinstance(team_other, TeamOtherStatsIngestor)

        team_advanced = create_ingestor("team_advanced_stats")
        assert team_advanced is not None
        assert isinstance(team_advanced, TeamAdvancedStatsIngestor)

        injuries = create_ingestor("injuries")
        assert injuries is not None
        assert isinstance(injuries, InjuryIngestor)

        contracts = create_ingestor("contracts")
        assert contracts is not None
        assert isinstance(contracts, ContractIngestor)
