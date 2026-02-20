"""Tests for LineupsIngestor."""

from unittest.mock import patch

import pytest

from nba_vault.ingestion.lineups import LineupsIngestor, generate_lineup_id
from nba_vault.models.advanced_stats import LineupCreate


@pytest.fixture
def mock_lineups_data():
    return {
        "Lineups": {
            "data": [[1, 2, 3, 4, 5, 100, 50, 100]],
            "headers": [
                "PLAYER_ID_1",
                "PLAYER_ID_2",
                "PLAYER_ID_3",
                "PLAYER_ID_4",
                "PLAYER_ID_5",
                "TEAM_ID",
                "MIN",
                "PTS",
            ],
        }
    }


@pytest.fixture
def mock_lineups_validate_data():
    return {
        "data": {
            "Lineups": {
                "data": [[1610612747, 1, 2, 3, 4, 5, 100.0, 50, 100, 95, 110.5, 105.2, 5.3]],
                "headers": [
                    "TEAM_ID",
                    "PLAYER_ID_1",
                    "PLAYER_ID_2",
                    "PLAYER_ID_3",
                    "PLAYER_ID_4",
                    "PLAYER_ID_5",
                    "MIN",
                    "POSS",
                    "PTS",
                    "PTS_ALLOWED",
                    "OFF_RATING",
                    "DEF_RATING",
                    "NET_RATING",
                ],
            }
        },
        "scope": "league",
        "season": "2023-24",
    }


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
    def test_fetch_league_lineups(self, mock_get_lineups, mock_lineups_data):
        """Test fetching all lineups in league."""
        mock_get_lineups.return_value = mock_lineups_data

        ingestor = LineupsIngestor()
        result = ingestor.fetch("league", season="2023-24")

        assert result["scope"] == "league"
        assert "data" in result

    def test_validate_with_data(self, mock_lineups_validate_data):
        """Test validating lineup data."""
        ingestor = LineupsIngestor()
        result = ingestor.validate(mock_lineups_validate_data)
        assert len(result) == 1
        assert isinstance(result[0], LineupCreate)
        assert result[0].team_id == 1610612747


class TestLineupsUpsert:
    """Integration tests for LineupsIngestor.upsert()."""

    def test_upsert_inserts_new_lineup(self, db_connection):
        ingestor = LineupsIngestor()
        models = [
            LineupCreate(
                lineup_id="LU0001",
                season_id=2023,
                team_id=1610612747,
                player_1_id=1,
                player_2_id=2,
                player_3_id=3,
                player_4_id=4,
                player_5_id=5,
                minutes_played=120.0,
                points_scored=250,
                points_allowed=240,
            )
        ]
        rows = ingestor.upsert(models, db_connection)
        assert rows == 1

        cursor = db_connection.execute("SELECT team_id FROM lineup WHERE lineup_id = 'LU0001'")
        assert cursor.fetchone() is not None

    def test_upsert_updates_existing_lineup(self, db_connection):
        ingestor = LineupsIngestor()
        base = LineupCreate(
            lineup_id="LU0002",
            season_id=2023,
            team_id=1610612747,
            player_1_id=10,
            player_2_id=20,
            player_3_id=30,
            player_4_id=40,
            player_5_id=50,
            minutes_played=50.0,
        )
        ingestor.upsert([base], db_connection)

        updated = LineupCreate(
            lineup_id="LU0002",
            season_id=2023,
            team_id=1610612747,
            player_1_id=10,
            player_2_id=20,
            player_3_id=30,
            player_4_id=40,
            player_5_id=50,
            minutes_played=99.0,
        )
        rows = ingestor.upsert([updated], db_connection)
        assert rows == 1

        cursor = db_connection.execute(
            "SELECT minutes_played FROM lineup WHERE lineup_id = 'LU0002' AND season_id = 2023"
        )
        assert cursor.fetchone()[0] == 99.0
