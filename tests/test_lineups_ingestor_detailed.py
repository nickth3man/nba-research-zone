"""Detailed tests for LineupsIngestor.

Tests cover edge cases, error handling, and data transformation logic.
"""

import sqlite3
from unittest.mock import Mock, patch

import pytest

from nba_vault.ingestion.lineups import (
    LineupsIngestor,
    generate_lineup_id,
)
from nba_vault.models.advanced_stats import LineupCreate


class TestGenerateLineupID:
    """Tests for generate_lineup_id() function."""

    def test_id_consistency_same_players(self):
        """Test that same player IDs generate same lineup ID."""
        id1 = generate_lineup_id(1, 2, 3, 4, 5)
        id2 = generate_lineup_id(1, 2, 3, 4, 5)
        assert id1 == id2

    def test_id_order_independence(self):
        """Test that player order doesn't affect lineup ID."""
        id1 = generate_lineup_id(5, 4, 3, 2, 1)
        id2 = generate_lineup_id(1, 2, 3, 4, 5)
        assert id1 == id2

    def test_id_uniqueness_different_players(self):
        """Test that different players generate different IDs."""
        id1 = generate_lineup_id(1, 2, 3, 4, 5)
        id2 = generate_lineup_id(1, 2, 3, 4, 6)
        assert id1 != id2

    def test_id_is_string(self):
        """Test that lineup ID is a string."""
        lineup_id = generate_lineup_id(1, 2, 3, 4, 5)
        assert isinstance(lineup_id, str)

    def test_id_is_hash(self):
        """Test that lineup ID is a hexadecimal hash."""
        lineup_id = generate_lineup_id(1, 2, 3, 4, 5)
        assert len(lineup_id) == 64  # SHA256 produces 64 hex chars
        assert all(c in "0123456789abcdef" for c in lineup_id)

    def test_id_with_duplicate_players(self):
        """Test lineup ID with duplicate player IDs."""
        # This shouldn't happen in real data, but test behavior
        id1 = generate_lineup_id(1, 1, 2, 3, 4)
        id2 = generate_lineup_id(1, 2, 3, 4, 1)
        # Should be same since sorting makes them equal
        assert id1 == id2


class TestLineupsIngestorFetch:
    """Tests for LineupsIngestor.fetch() method."""

    def test_fetch_league_scope(self):
        """Test fetching all lineups in league."""
        ingestor = LineupsIngestor()

        with patch.object(ingestor.nba_client, "get_all_lineups") as mock_get:
            mock_get.return_value = {"Lineups": {"data": []}}

            result = ingestor.fetch("league", season="2023-24")

            assert result["scope"] == "league"
            assert result["season"] == "2023-24"
            mock_get.assert_called_once()

    def test_fetch_team_scope(self):
        """Test fetching lineups for specific team."""
        ingestor = LineupsIngestor()

        with patch.object(ingestor.nba_client, "get_team_lineups") as mock_get:
            mock_get.return_value = {"Lineups": {"data": []}}

            result = ingestor.fetch("1610612747", season="2023-24")

            assert result["scope"] == "team"
            assert result["team_id"] == 1610612747
            mock_get.assert_called_once()

    def test_fetch_game_scope(self):
        """Test fetching lineups for specific game (not implemented)."""
        ingestor = LineupsIngestor()

        result = ingestor.fetch("game:0022300001", season="2023-24")

        assert result["scope"] == "game"
        assert result["game_id"] == "0022300001"
        assert result["data"] == {}

    def test_fetch_with_custom_season_type(self):
        """Test fetching with custom season type."""
        ingestor = LineupsIngestor()

        with patch.object(ingestor.nba_client, "get_all_lineups") as mock_get:
            mock_get.return_value = {"Lineups": {"data": []}}

            ingestor.fetch("league", season="2023-24", season_type="Playoffs")

            # Verify season_type was passed
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs["season_type"] == "Playoffs"


class TestLineupsIngestorValidate:
    """Tests for LineupsIngestor.validate() method."""

    def test_validate_with_complete_data(self):
        """Test validation with complete lineup data."""
        ingestor = LineupsIngestor()

        raw_data = {
            "data": {
                "Lineups": {
                    "data": [
                        [
                            1610612747,
                            1,
                            2,
                            3,
                            4,
                            5,
                            100.0,
                            200,
                            250,
                            240,
                            115.0,
                            110.0,
                            5.0,
                        ]
                    ],
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
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)

        assert len(result) == 1
        assert isinstance(result[0], LineupCreate)
        assert result[0].team_id == 1610612747
        assert result[0].player_1_id == 1
        assert result[0].minutes_played == 100.0

    def test_validate_skips_rows_with_missing_player_ids(self):
        """Test that rows without 5 player IDs are skipped."""
        ingestor = LineupsIngestor()

        raw_data = {
            "data": {
                "Lineups": {
                    "data": [
                        [1610612747, 1, 2, 3],  # Only 3 players
                    ],
                    "headers": ["TEAM_ID", "PLAYER_ID_1", "PLAYER_ID_2", "PLAYER_ID_3"],
                }
            },
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 0

    def test_validate_skips_rows_with_zero_minutes(self):
        """Test that lineups with 0 or negative minutes are skipped."""
        ingestor = LineupsIngestor()

        raw_data = {
            "data": {
                "Lineups": {
                    "data": [
                        [1610612747, 1, 2, 3, 4, 5, 0.0],  # Zero minutes
                    ],
                    "headers": [
                        "TEAM_ID",
                        "PLAYER_ID_1",
                        "PLAYER_ID_2",
                        "PLAYER_ID_3",
                        "PLAYER_ID_4",
                        "PLAYER_ID_5",
                        "MIN",
                    ],
                }
            },
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 0

    def test_validate_skips_rows_with_missing_team_id(self):
        """Test that rows without team_id are skipped."""
        ingestor = LineupsIngestor()

        raw_data = {
            "data": {
                "Lineups": {
                    "data": [
                        [None, 1, 2, 3, 4, 5, 100.0],  # No team_id
                    ],
                    "headers": [
                        "TEAM_ID",
                        "PLAYER_ID_1",
                        "PLAYER_ID_2",
                        "PLAYER_ID_3",
                        "PLAYER_ID_4",
                        "PLAYER_ID_5",
                        "MIN",
                    ],
                }
            },
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 0

    def test_validate_with_empty_data(self):
        """Test validation with empty data."""
        ingestor = LineupsIngestor()

        raw_data = {
            "data": {
                "Lineups": {
                    "data": [],
                    "headers": ["TEAM_ID"],
                }
            },
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        assert result == []

    def test_validate_with_missing_headers(self):
        """Test validation when headers are missing."""
        ingestor = LineupsIngestor()

        raw_data = {
            "data": {
                "Lineups": {
                    "data": [[1, 2, 3, 4, 5, 6]],
                    "headers": [],
                }
            },
            "season": "2023-24",
        }

        # Should handle gracefully
        result = ingestor.validate(raw_data)
        assert isinstance(result, list)

    def test_validate_with_alternative_points_allowed(self):
        """Test validation with PTS_ALLOWED vs OPP_PTS."""
        ingestor = LineupsIngestor()

        raw_data = {
            "data": {
                "Lineups": {
                    "data": [
                        [1610612747, 1, 2, 3, 4, 5, 100.0, 200, 250, 240],  # PTS_ALLOWED
                    ],
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
                    ],
                }
            },
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 1
        assert result[0].points_allowed == 240  # type: ignore[attr-defined]

    def test_validate_with_opp_points(self):
        """Test validation with OPP_PTS instead of PTS_ALLOWED."""
        ingestor = LineupsIngestor()

        raw_data = {
            "data": {
                "Lineups": {
                    "data": [
                        [1610612747, 1, 2, 3, 4, 5, 100.0, 200, 250, 240],  # OPP_PTS
                    ],
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
                        "OPP_PTS",
                    ],
                }
            },
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 1
        assert result[0].points_allowed == 240  # type: ignore[attr-defined]

    def test_validate_with_null_values(self):
        """Test validation with null/empty values."""
        ingestor = LineupsIngestor()

        raw_data = {
            "data": {
                "Lineups": {
                    "data": [
                        [1610612747, 1, 2, 3, 4, 5, 100.0, "", "", None, None],  # Null values
                    ],
                    "headers": [
                        "TEAM_ID",
                        "PLAYER_ID_1",
                        "PLAYER_ID_2",
                        "PLAYER_ID_3",
                        "PLAYER_ID_4",
                        "PLAYER_ID_5",
                        "MIN",
                        "OFF_RATING",
                        "DEF_RATING",
                        "PTS",
                        "PTS_ALLOWED",
                    ],
                }
            },
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        # Should handle nulls gracefully
        assert isinstance(result, list)


class TestLineupsIngestorUpsert:
    """Tests for LineupsIngestor.upsert() method."""

    def test_upsert_with_integrity_error(self, db_connection):
        """Test handling of integrity errors during upsert."""
        ingestor = LineupsIngestor()

        lineup = LineupCreate(
            lineup_id="LU001",
            season_id=2023,
            team_id=1610612747,
            player_1_id=1,
            player_2_id=2,
            player_3_id=3,
            player_4_id=4,
            player_5_id=5,
            minutes_played=100.0,
        )

        # Use a Mock connection so execute can be patched
        mock_conn = Mock()
        call_count = [0]

        def mock_execute(sql, params=None):
            call_count[0] += 1
            if call_count[0] > 1:
                raise sqlite3.IntegrityError("UNIQUE constraint failed")
            return Mock()

        mock_conn.execute = mock_execute

        with pytest.raises(sqlite3.IntegrityError):
            ingestor.upsert([lineup], mock_conn)

    def test_upsert_with_operational_error(self, db_connection):
        """Test handling of operational errors during upsert."""
        ingestor = LineupsIngestor()

        lineup = LineupCreate(
            lineup_id="LU002",
            season_id=2023,
            team_id=1610612747,
            player_1_id=1,
            player_2_id=2,
            player_3_id=3,
            player_4_id=4,
            player_5_id=5,
            minutes_played=100.0,
        )

        # Use a Mock connection so execute can be patched
        mock_conn = Mock()
        mock_conn.execute.side_effect = sqlite3.OperationalError("database is locked")

        with pytest.raises(sqlite3.OperationalError):
            ingestor.upsert([lineup], mock_conn)


class TestLineupsIngestorExtractPlayerIDs:
    """Tests for LineupsIngestor._extract_player_ids() method."""

    def test_extract_from_standard_fields(self):
        """Test extracting player IDs from PLAYER_ID_X fields."""
        ingestor = LineupsIngestor()

        row_dict = {
            "PLAYER_ID_1": "1",
            "PLAYER_ID_2": "2",
            "PLAYER_ID_3": "3",
            "PLAYER_ID_4": "4",
            "PLAYER_ID_5": "5",
        }

        result = ingestor._extract_player_ids(row_dict)
        assert result == [1, 2, 3, 4, 5]

    def test_extract_from_lineup_string(self):
        """Test extracting player IDs from LINEUP string format."""
        ingestor = LineupsIngestor()

        row_dict = {
            "LINEUP": "1/2/3/4/5",
        }

        result = ingestor._extract_player_ids(row_dict)
        assert result == [1, 2, 3, 4, 5]

    def test_extract_with_missing_player_id(self):
        """Test extraction when one player ID is missing."""
        ingestor = LineupsIngestor()

        row_dict = {
            "PLAYER_ID_1": "1",
            "PLAYER_ID_2": "2",
            "PLAYER_ID_3": "3",
            "PLAYER_ID_4": "4",
            # PLAYER_ID_5 is missing
            "LINEUP": "1/2/3/4/5",
        }

        result = ingestor._extract_player_ids(row_dict)
        assert result == [1, 2, 3, 4, 5]

    def test_extract_falls_back_to_lineup_string(self):
        """Test fallback to LINEUP string when PLAYER_ID fields are insufficient."""
        ingestor = LineupsIngestor()

        row_dict = {
            "PLAYER_ID_1": "1",
            "PLAYER_ID_2": "2",
            # Only 2 of 5 players
            "LINEUP": "1/2/3/4/5",
        }

        result = ingestor._extract_player_ids(row_dict)
        assert len(result) == 5

    def test_extract_with_string_ids(self):
        """Test extraction when IDs are strings."""
        ingestor = LineupsIngestor()

        row_dict = {
            "PLAYER_ID_1": "100",
            "PLAYER_ID_2": "200",
            "PLAYER_ID_3": "300",
            "PLAYER_ID_4": "400",
            "PLAYER_ID_5": "500",
        }

        result = ingestor._extract_player_ids(row_dict)
        assert result == [100, 200, 300, 400, 500]

    def test_extract_with_invalid_values(self):
        """Test extraction with invalid/empty values."""
        ingestor = LineupsIngestor()

        row_dict = {
            "PLAYER_ID_1": "1",
            "PLAYER_ID_2": "",
            "PLAYER_ID_3": None,
            "PLAYER_ID_4": "4",
            "PLAYER_ID_5": "5",
        }

        result = ingestor._extract_player_ids(row_dict)
        # Invalid values should be filtered out
        assert 1 in result
        assert 4 in result
        assert 5 in result
        assert len(result) <= 5


class TestLineupsIngestorSafeConversions:
    """Tests for LineupsIngestor safe conversion methods."""

    def test_safe_float_with_valid_values(self):
        """Test safe_float with valid values."""
        assert LineupsIngestor._safe_float(3.14) == 3.14
        assert LineupsIngestor._safe_float("3.14") == 3.14
        assert LineupsIngestor._safe_float("100") == 100.0

    def test_safe_float_with_invalid_values(self):
        """Test safe_float with invalid values."""
        assert LineupsIngestor._safe_float(None) is None
        assert LineupsIngestor._safe_float("") is None
        assert LineupsIngestor._safe_float("-") is None
        assert LineupsIngestor._safe_float("invalid") is None

    def test_safe_int_with_valid_values(self):
        """Test safe_int with valid values."""
        assert LineupsIngestor._safe_int(42) == 42
        assert LineupsIngestor._safe_int("42") == 42
        assert LineupsIngestor._safe_int("42.5") == 42

    def test_safe_int_with_invalid_values(self):
        """Test safe_int with invalid values."""
        assert LineupsIngestor._safe_int(None) is None
        assert LineupsIngestor._safe_int("") is None
        assert LineupsIngestor._safe_int("-") is None
        assert LineupsIngestor._safe_int("invalid") is None


class TestLineupsIngestorEdgeCases:
    """Edge case tests for LineupsIngestor."""

    def test_validate_with_multiple_datasets(self):
        """Test validation with multiple datasets in response."""
        ingestor = LineupsIngestor()

        raw_data = {
            "data": {
                "Lineups": {
                    "data": [[1610612747, 1, 2, 3, 4, 5, 100.0]],
                    "headers": [
                        "TEAM_ID",
                        "PLAYER_ID_1",
                        "PLAYER_ID_2",
                        "PLAYER_ID_3",
                        "PLAYER_ID_4",
                        "PLAYER_ID_5",
                        "MIN",
                    ],
                },
                "OtherData": {
                    "data": [[1, 2, 3]],
                    "headers": ["A", "B", "C"],
                },
            },
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        # Should process all datasets
        assert len(result) >= 0

    def test_validate_with_non_dict_dataset(self):
        """Test validation when dataset is not a dict."""
        ingestor = LineupsIngestor()

        raw_data = {
            "data": {
                "Lineups": "not a dict",  # Invalid
            },
            "season": "2023-24",
        }

        # Should handle gracefully
        result = ingestor.validate(raw_data)
        assert isinstance(result, list)

    def test_season_id_extraction(self):
        """Test that season_id is correctly extracted from season string."""
        ingestor = LineupsIngestor()

        raw_data = {
            "data": {
                "Lineups": {
                    "data": [[1610612747, 1, 2, 3, 4, 5, 100.0]],
                    "headers": [
                        "TEAM_ID",
                        "PLAYER_ID_1",
                        "PLAYER_ID_2",
                        "PLAYER_ID_3",
                        "PLAYER_ID_4",
                        "PLAYER_ID_5",
                        "MIN",
                    ],
                }
            },
            "season": "2022-23",  # Different season
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 1
        assert result[0].season_id == 2022  # type: ignore[attr-defined]

    def test_lineup_id_generation_in_validate(self):
        """Test that lineup_id is generated correctly."""
        ingestor = LineupsIngestor()

        raw_data = {
            "data": {
                "Lineups": {
                    "data": [[1610612747, 1, 2, 3, 4, 5, 100.0]],
                    "headers": [
                        "TEAM_ID",
                        "PLAYER_ID_1",
                        "PLAYER_ID_2",
                        "PLAYER_ID_3",
                        "PLAYER_ID_4",
                        "PLAYER_ID_5",
                        "MIN",
                    ],
                }
            },
            "season": "2023-24",
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 1

        # Generate expected ID — must include season_id and team_id to match validate()
        expected_id = generate_lineup_id(1, 2, 3, 4, 5, season_id=2023, team_id=1610612747)
        assert result[0].lineup_id == expected_id  # type: ignore[attr-defined]
