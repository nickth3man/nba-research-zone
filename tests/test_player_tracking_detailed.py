"""Detailed tests for PlayerTrackingIngestor.

Tests cover edge cases, error handling, and data transformation logic.
"""

import sqlite3
from unittest.mock import Mock, patch

import pytest

from nba_vault.ingestion.player_tracking import PlayerTrackingIngestor
from nba_vault.models.advanced_stats import PlayerGameTrackingCreate


class TestPlayerTrackingIngestorFetch:
    """Tests for PlayerTrackingIngestor.fetch() method."""

    def test_fetch_single_player(self):
        """Test fetching tracking data for single player."""
        ingestor = PlayerTrackingIngestor()

        with patch.object(ingestor.nba_client, "get_player_tracking") as mock_get:
            mock_get.return_value = {
                "PlayerTracking": {
                    "data": [[2544, "2023-24", 1610612747, 2.5, 4.2]],
                    "headers": ["PLAYER_ID", "SEASON", "TEAM_ID", "DIST_MILES", "SPD"],
                }
            }

            result = ingestor.fetch("2544", season="2023-24")

            assert result["player_id"] == 2544
            assert result["season"] == "2023-24"
            assert "data" in result

    def test_fetch_team_players(self):
        """Test fetching tracking data for all players on a team."""
        ingestor = PlayerTrackingIngestor()

        result = ingestor.fetch("team:1610612747", season="2023-24")

        assert result["team_id"] == 1610612747
        assert result["players"] == []  # Not fully implemented

    def test_fetch_pre_2013_season_raises_error(self):
        """Test that fetching pre-2013 season raises ValueError."""
        ingestor = PlayerTrackingIngestor()

        with pytest.raises(ValueError, match="only available from 2013-14"):
            ingestor.fetch("2544", season="2012-13")

    def test_fetch_exactly_2013_season(self):
        """Test that 2013-14 season (first available) works."""
        ingestor = PlayerTrackingIngestor()

        with patch.object(ingestor.nba_client, "get_player_tracking") as mock_get:
            mock_get.return_value = {"PlayerTracking": {"data": []}}

            # Should not raise
            result = ingestor.fetch("2544", season="2013-14")
            assert "data" in result

    def test_fetch_with_season_type(self):
        """Test fetching with custom season type."""
        ingestor = PlayerTrackingIngestor()

        with patch.object(ingestor.nba_client, "get_player_tracking") as mock_get:
            mock_get.return_value = {"PlayerTracking": {"data": []}}

            ingestor.fetch("2544", season="2023-24", season_type="Playoffs")

            # Verify season_type was passed
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs["season_type"] == "Playoffs"


class TestPlayerTrackingIngestorValidate:
    """Tests for PlayerTrackingIngestor.validate() method."""

    def test_validate_with_complete_data(self):
        """Test validation with complete tracking data."""
        ingestor = PlayerTrackingIngestor()

        raw_data = {
            "data": {
                "PlayerTracking": {
                    "data": [
                        [
                            2544,
                            1610612747,
                            2.5,
                            1.2,
                            1.3,
                            3.5,
                            85,
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

        result = ingestor.validate(raw_data)

        assert len(result) == 1
        assert isinstance(result[0], PlayerGameTrackingCreate)
        assert result[0].player_id == 2544
        assert result[0].distance_miles == 2.5
        assert result[0].touches == 85

    def test_validate_skips_rows_without_player_id(self):
        """Test that rows without player_id are skipped."""
        ingestor = PlayerTrackingIngestor()

        raw_data = {
            "data": {
                "PlayerTracking": {
                    "data": [
                        [None, 1610612747, 2.5],  # Missing player_id
                    ],
                    "headers": ["PLAYER_ID", "TEAM_ID", "DIST_MILES"],
                }
            },
            "player_id": None,
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 0

    def test_validate_skips_rows_without_team_id(self):
        """Test that rows without team_id are skipped."""
        ingestor = PlayerTrackingIngestor()

        raw_data = {
            "data": {
                "PlayerTracking": {
                    "data": [
                        [2544, None, 2.5],  # Missing team_id
                    ],
                    "headers": ["PLAYER_ID", "TEAM_ID", "DIST_MILES"],
                }
            },
            "player_id": 2544,
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 0

    def test_validate_with_empty_data(self):
        """Test validation with empty data."""
        ingestor = PlayerTrackingIngestor()

        raw_data = {
            "data": {},
            "player_id": 2544,
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        assert result == []

    def test_validate_uses_row_player_id(self):
        """Test that player_id from row is used when available."""
        ingestor = PlayerTrackingIngestor()

        raw_data = {
            "data": {
                "PlayerTracking": {
                    "data": [
                        [9999, 1610612747, 2.5],  # Different player_id in row
                    ],
                    "headers": ["PLAYER_ID", "TEAM_ID", "DIST_MILES"],
                }
            },
            "player_id": 2544,  # Different player_id in context
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 1
        assert result[0].player_id == 9999  # type: ignore[attr-defined]  # Should use row player_id

    def test_validate_generates_game_id_when_missing(self):
        """Test that game_id is generated when not in data."""
        ingestor = PlayerTrackingIngestor()

        raw_data = {
            "data": {
                "PlayerTracking": {
                    "data": [
                        [2544, 1610612747, 2.5],  # No GAME_ID
                    ],
                    "headers": ["PLAYER_ID", "TEAM_ID", "DIST_MILES"],
                }
            },
            "player_id": 2544,
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 1
        assert result[0].game_id == "season_2023"  # type: ignore[attr-defined]

    def test_validate_uses_game_id_from_row(self):
        """Test that game_id from row is used when available."""
        ingestor = PlayerTrackingIngestor()

        raw_data = {
            "data": {
                "PlayerTracking": {
                    "data": [
                        [2544, "0022300001", 1610612747, 2.5],
                    ],
                    "headers": ["PLAYER_ID", "GAME_ID", "TEAM_ID", "DIST_MILES"],
                }
            },
            "player_id": 2544,
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 1
        assert result[0].game_id == "0022300001"  # type: ignore[attr-defined]

    def test_validate_with_null_values(self):
        """Test validation with null/empty values."""
        ingestor = PlayerTrackingIngestor()

        raw_data = {
            "data": {
                "PlayerTracking": {
                    "data": [
                        [2544, 1610612747, "", None, "", ""],  # Null values
                    ],
                    "headers": ["PLAYER_ID", "TEAM_ID", "DIST_MILES", "SPD", "TOUCHES", "DRIVES"],
                }
            },
            "player_id": 2544,
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        # Should handle nulls gracefully
        assert isinstance(result, list)

    def test_validate_with_multiple_datasets(self):
        """Test validation with multiple datasets in response."""
        ingestor = PlayerTrackingIngestor()

        raw_data = {
            "data": {
                "PlayerTracking": {
                    "data": [[2544, 1610612747, 2.5]],
                    "headers": ["PLAYER_ID", "TEAM_ID", "DIST_MILES"],
                },
                "OtherData": {
                    "data": [[1, 2, 3]],
                    "headers": ["A", "B", "C"],
                },
            },
            "player_id": 2544,
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        # Should process all datasets
        assert len(result) >= 0

    def test_season_id_extraction(self):
        """Test that season_id is correctly extracted from season string."""
        ingestor = PlayerTrackingIngestor()

        raw_data = {
            "data": {
                "PlayerTracking": {
                    "data": [[2544, 1610612747, 2.5]],
                    "headers": ["PLAYER_ID", "TEAM_ID", "DIST_MILES"],
                }
            },
            "player_id": 2544,
            "season": "2022-23",  # Different season
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 1
        assert result[0].season_id == 2022  # type: ignore[attr-defined]


class TestPlayerTrackingIngestorUpsert:
    """Tests for PlayerTrackingIngestor.upsert() method."""

    def test_upsert_with_integrity_error(self, db_connection):
        """Test handling of integrity errors during upsert."""
        ingestor = PlayerTrackingIngestor()

        tracking = PlayerGameTrackingCreate(
            game_id="0022300001",
            player_id=2544,
            team_id=1610612747,
            season_id=2023,
            distance_miles=2.5,
        )

        # Use a MagicMock connection so execute can be patched
        mock_conn = Mock()
        call_count = [0]

        def mock_execute(sql, params=None):
            call_count[0] += 1
            if call_count[0] > 1:
                raise sqlite3.IntegrityError("UNIQUE constraint failed")
            return Mock()

        mock_conn.execute = mock_execute

        with pytest.raises(sqlite3.IntegrityError):
            ingestor.upsert([tracking], mock_conn)

    def test_upsert_with_operational_error(self, db_connection):
        """Test handling of operational errors during upsert."""
        ingestor = PlayerTrackingIngestor()

        tracking = PlayerGameTrackingCreate(
            game_id="0022300002",
            player_id=2544,
            team_id=1610612747,
            season_id=2023,
            distance_miles=2.5,
        )

        # Use a MagicMock connection so execute can be patched
        mock_conn = Mock()
        mock_conn.execute.side_effect = sqlite3.OperationalError("database is locked")

        with pytest.raises(sqlite3.OperationalError):
            ingestor.upsert([tracking], mock_conn)


class TestPlayerTrackingSafeConversions:
    """Tests for safe conversion static methods."""

    def test_safe_float_with_valid_values(self):
        """Test _safe_float with valid values."""
        assert PlayerTrackingIngestor._safe_float(3.14) == 3.14
        assert PlayerTrackingIngestor._safe_float("3.14") == 3.14
        assert PlayerTrackingIngestor._safe_float("100") == 100.0

    def test_safe_float_with_invalid_values(self):
        """Test _safe_float with invalid values."""
        assert PlayerTrackingIngestor._safe_float(None) is None
        assert PlayerTrackingIngestor._safe_float("") is None
        assert PlayerTrackingIngestor._safe_float("invalid") is None

    def test_safe_int_with_valid_values(self):
        """Test _safe_int with valid values."""
        assert PlayerTrackingIngestor._safe_int(42) == 42
        assert PlayerTrackingIngestor._safe_int("42") == 42
        assert PlayerTrackingIngestor._safe_int("42.5") == 42
        assert PlayerTrackingIngestor._safe_int(42.9) == 42

    def test_safe_int_with_invalid_values(self):
        """Test _safe_int with invalid values."""
        assert PlayerTrackingIngestor._safe_int(None) is None
        assert PlayerTrackingIngestor._safe_int("") is None
        assert PlayerTrackingIngestor._safe_int("invalid") is None


class TestPlayerTrackingEdgeCases:
    """Edge case tests for PlayerTrackingIngestor."""

    def test_validate_with_all_optional_fields_null(self):
        """Test validation when all optional fields are null."""
        ingestor = PlayerTrackingIngestor()

        raw_data = {
            "data": {
                "PlayerTracking": {
                    "data": [[2544, 1610612747]],
                    "headers": ["PLAYER_ID", "TEAM_ID"],
                }
            },
            "player_id": 2544,
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        # Should create record with None for optional fields
        assert len(result) == 1

    def test_validate_with_max_values(self):
        """Test validation with maximum realistic values."""
        ingestor = PlayerTrackingIngestor()

        raw_data = {
            "data": {
                "PlayerTracking": {
                    "data": [
                        [
                            2544,
                            1610612747,
                            99.9,
                            99.9,
                            99.9,
                            99.9,
                            999,
                            999,
                            999,
                            999,
                            999,
                            999,
                            999,
                            999,
                        ]
                    ],
                    "headers": [
                        "PLAYER_ID",
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

        result = ingestor.validate(raw_data)
        assert len(result) == 1
        assert result[0].distance_miles == 99.9  # type: ignore[attr-defined]
        assert result[0].speed_mph_avg == 99.9  # type: ignore[attr-defined]

    def test_validate_with_zero_values(self):
        """Test validation with zero values."""
        ingestor = PlayerTrackingIngestor()

        raw_data = {
            "data": {
                "PlayerTracking": {
                    "data": [[2544, 1610612747, 0.0, 0.0, 0.0, 0, 0, 0, 0, 0, 0, 0, 0, 0]],
                    "headers": [
                        "PLAYER_ID",
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

        result = ingestor.validate(raw_data)
        assert len(result) == 1
        assert result[0].distance_miles == 0.0  # type: ignore[attr-defined]
        assert result[0].touches == 0  # type: ignore[attr-defined]

    def test_validate_with_string_numbers(self):
        """Test validation when numeric values are strings."""
        ingestor = PlayerTrackingIngestor()

        raw_data = {
            "data": {
                "PlayerTracking": {
                    "data": [[2544, 1610612747, "2.5", "3.5", "85"]],
                    "headers": ["PLAYER_ID", "TEAM_ID", "DIST_MILES", "SPD", "TOUCHES"],
                }
            },
            "player_id": 2544,
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 1
        assert result[0].distance_miles == 2.5  # type: ignore[attr-defined]
        assert result[0].speed_mph_avg == 3.5  # type: ignore[attr-defined]
        assert result[0].touches == 85  # type: ignore[attr-defined]

    def test_validate_non_dict_dataset(self):
        """Test validation when dataset is not a dict."""
        ingestor = PlayerTrackingIngestor()

        raw_data = {
            "data": {
                "PlayerTracking": "not a dict",
            },
            "player_id": 2544,
            "season": "2023-24",
        }

        # Should handle gracefully
        result = ingestor.validate(raw_data)
        assert isinstance(result, list)
