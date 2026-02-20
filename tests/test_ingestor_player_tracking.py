"""Tests for PlayerTrackingIngestor."""

from unittest.mock import patch

import pytest

from nba_vault.ingestion.player_tracking import PlayerTrackingIngestor
from nba_vault.models.advanced_stats import PlayerGameTrackingCreate


@pytest.fixture
def mock_tracking_data():
    return {
        "PlayerTracking": {
            "data": [[1, "2023-24", 100, 2.5, 1.2, 1.3]],
            "headers": ["PLAYER_ID", "SEASON", "TEAM_ID", "DIST_MILES", "SPD", "MIN"],
        }
    }


@pytest.fixture
def mock_tracking_validate_data():
    return {
        "data": {
            "PlayerTracking": {
                "data": [
                    [
                        2544,
                        "2023-24",
                        1610612747,
                        2.5,
                        4.2,
                        1.2,
                        1.3,
                        50,
                        10,
                        5,
                        2,
                        20,
                        15,
                        15,
                        10,
                    ]
                ],
                "headers": [
                    "PLAYER_ID",
                    "SEASON",
                    "TEAM_ID",
                    "DIST_MILES",
                    "DIST_MILES_OFF",
                    "DIST_MILES_DEF",
                    "SPD",
                    "TOUCHES",
                    "EFC",
                    "PAINT",
                    "POST",
                    "DRIVES",
                    "DRIVES_PTS",
                    "PULL_UP_FGA",
                    "PULL_UP_FGM",
                ],
            }
        },
        "player_id": 2544,
        "season": "2023-24",
    }


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
    def test_fetch_single_player(self, mock_get_tracking, mock_tracking_data):
        """Test fetching tracking data for single player."""
        mock_get_tracking.return_value = mock_tracking_data

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

    def test_validate_with_data(self, mock_tracking_validate_data):
        """Test validating tracking data."""
        ingestor = PlayerTrackingIngestor()
        result = ingestor.validate(mock_tracking_validate_data)
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


class TestPlayerTrackingUpsert:
    """Integration tests for PlayerTrackingIngestor.upsert()."""

    def test_upsert_inserts_new_record(self, db_connection):
        ingestor = PlayerTrackingIngestor()
        models = [
            PlayerGameTrackingCreate(
                game_id="0022300001",
                player_id=2544,
                team_id=1610612747,
                season_id=2023,
                minutes_played=38.5,
                distance_miles=3.2,
                touches=85,
            )
        ]
        rows = ingestor.upsert(models, db_connection)
        assert rows == 1

        cursor = db_connection.execute(
            "SELECT player_id FROM player_game_tracking WHERE game_id = '0022300001'"
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == 2544

    def test_upsert_updates_existing_record(self, db_connection):
        ingestor = PlayerTrackingIngestor()
        model = PlayerGameTrackingCreate(
            game_id="0022300002",
            player_id=201939,
            team_id=1610612738,
            season_id=2023,
            minutes_played=32.0,
            distance_miles=2.8,
        )
        ingestor.upsert([model], db_connection)

        # Re-upsert with updated distance
        model2 = PlayerGameTrackingCreate(
            game_id="0022300002",
            player_id=201939,
            team_id=1610612738,
            season_id=2023,
            minutes_played=32.0,
            distance_miles=3.1,
        )
        rows = ingestor.upsert([model2], db_connection)
        assert rows == 1

        cursor = db_connection.execute(
            "SELECT distance_miles FROM player_game_tracking "
            "WHERE game_id = '0022300002' AND player_id = 201939"
        )
        assert cursor.fetchone()[0] == 3.1

    def test_upsert_skips_non_tracking_models(self, db_connection):
        from pydantic import BaseModel

        class Other(BaseModel):
            x: int = 1

        ingestor = PlayerTrackingIngestor()
        rows = ingestor.upsert([Other()], db_connection)
        assert rows == 0
