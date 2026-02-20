"""Tests for TeamOtherStatsIngestor and TeamAdvancedStatsIngestor."""

from unittest.mock import patch

import pytest

from nba_vault.ingestion.team_advanced_stats import TeamAdvancedStatsIngestor
from nba_vault.ingestion.team_other_stats import TeamOtherStatsIngestor
from nba_vault.models.advanced_stats import TeamGameOtherStatsCreate, TeamSeasonAdvancedCreate


@pytest.fixture
def mock_team_other_data():
    return {
        "OtherStats": {
            "data": [[1610612747, 50, 20, 15, 10, 5, 3, 8, 15, 20]],
            "headers": [
                "TEAM_ID",
                "PTS_PAINT",
                "PTS_2ND_CHANCE",
                "PTS_FB",
                "LARGEST_LEAD",
                "LEAD_CHANGES",
                "TIMES_TIED",
                "TEAM_TO",
                "TEAM_REB",
                "PTS_OFF_TO",
            ],
        }
    }


@pytest.fixture
def mock_team_advanced_data():
    return {
        "TeamStats": {
            "data": [[1610612747, 115.2, 110.5, 4.7, 100.5, 0.520, 12.3, 25.4, 0.25, 0.35, 0.580]],
            "headers": [
                "TEAM_ID",
                "OFF_RATING",
                "DEF_RATING",
                "NET_RATING",
                "PACE",
                "EFG_PCT",
                "TM_TOV_PCT",
                "OREB_PCT",
                "FT_RATE",
                "FG3A_RATE",
                "TS_PCT",
            ],
        }
    }


@pytest.fixture
def mock_team_advanced_validate_data():
    return {
        "data": {
            "TeamStats": {
                "data": [
                    [1610612747, 115.2, 110.5, 4.7, 100.5, 0.520, 12.3, 25.4, 0.25, 0.35, 0.580]
                ],
                "headers": [
                    "TEAM_ID",
                    "OFF_RATING",
                    "DEF_RATING",
                    "NET_RATING",
                    "PACE",
                    "EFG_PCT",
                    "TM_TOV_PCT",
                    "OREB_PCT",
                    "FTA_RATE",
                    "FG3A_RATE",
                    "TS_PCT",
                ],
            }
        },
        "scope": "league",
        "season": "2023-24",
    }


class TestTeamOtherStatsIngestor:
    """Tests for TeamOtherStatsIngestor."""

    def test_init(self):
        """Test ingestor initialization."""
        ingestor = TeamOtherStatsIngestor()
        assert ingestor.entity_type == "team_other_stats"
        assert ingestor.nba_client is not None

    @patch("nba_vault.ingestion.nba_stats_client.NBAStatsClient.get_box_score_summary")
    def test_fetch_game_stats(self, mock_get_boxscore, mock_team_other_data):
        """Test fetching other stats for a game."""
        mock_get_boxscore.return_value = mock_team_other_data

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
    def test_fetch_league_stats(self, mock_get_stats, mock_team_advanced_data):
        """Test fetching advanced stats for all teams."""
        mock_get_stats.return_value = mock_team_advanced_data

        ingestor = TeamAdvancedStatsIngestor()
        result = ingestor.fetch("league", season="2023-24")

        assert result["scope"] == "league"
        assert "data" in result

    def test_validate_with_data(self, mock_team_advanced_validate_data):
        """Test validating advanced stats data."""
        ingestor = TeamAdvancedStatsIngestor()
        result = ingestor.validate(mock_team_advanced_validate_data)

        assert len(result) == 1
        assert isinstance(result[0], TeamSeasonAdvancedCreate)
        assert result[0].team_id == 1610612747
        assert result[0].off_rating == 115.2


class TestTeamOtherStatsUpsert:
    """Integration tests for TeamOtherStatsIngestor.upsert()."""

    def test_upsert_inserts_new_record(self, db_connection):
        ingestor = TeamOtherStatsIngestor()
        models = [
            TeamGameOtherStatsCreate(
                game_id="0022300003",
                team_id=1610612747,
                season_id=2023,
                points_paint=52,
                points_fast_break=18,
                largest_lead=20,
            )
        ]
        rows = ingestor.upsert(models, db_connection)
        assert rows == 1

    def test_upsert_updates_existing_record(self, db_connection):
        ingestor = TeamOtherStatsIngestor()
        model = TeamGameOtherStatsCreate(
            game_id="0022300004",
            team_id=1610612738,
            season_id=2023,
            points_paint=40,
        )
        ingestor.upsert([model], db_connection)

        updated = TeamGameOtherStatsCreate(
            game_id="0022300004",
            team_id=1610612738,
            season_id=2023,
            points_paint=55,
        )
        rows = ingestor.upsert([updated], db_connection)
        assert rows == 1


class TestTeamAdvancedStatsUpsert:
    """Integration tests for TeamAdvancedStatsIngestor.upsert()."""

    def test_upsert_inserts_new_record(self, db_connection):
        ingestor = TeamAdvancedStatsIngestor()
        models = [
            TeamSeasonAdvancedCreate(
                team_id=1610612747,
                season_id=2023,
                off_rating=115.0,
                def_rating=110.0,
                net_rating=5.0,
                pace=100.0,
            )
        ]
        rows = ingestor.upsert(models, db_connection)
        assert rows == 1

        cursor = db_connection.execute(
            "SELECT off_rating FROM team_season_advanced "
            "WHERE team_id = 1610612747 AND season_id = 2023"
        )
        assert cursor.fetchone()[0] == 115.0

    def test_upsert_updates_existing_record(self, db_connection):
        ingestor = TeamAdvancedStatsIngestor()
        model = TeamSeasonAdvancedCreate(
            team_id=1610612738,
            season_id=2023,
            off_rating=112.0,
            def_rating=108.0,
        )
        ingestor.upsert([model], db_connection)

        updated = TeamSeasonAdvancedCreate(
            team_id=1610612738,
            season_id=2023,
            off_rating=116.0,
        )
        rows = ingestor.upsert([updated], db_connection)
        assert rows == 1
