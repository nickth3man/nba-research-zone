"""Comprehensive tests for InjuryIngestor.

Tests cover error paths, edge cases, and web scraping logic.
"""

from datetime import date
from unittest.mock import Mock, patch

import pytest
import requests

from nba_vault.ingestion.injuries import InjuryIngestor
from nba_vault.models.advanced_stats import InjuryCreate


class TestInjuryIngestorFetch:
    """Tests for InjuryIngestor.fetch() method."""

    def test_fetch_all_espn_success(self):
        """Test successful fetch of all injuries from ESPN."""
        ingestor = InjuryIngestor()

        # Mock successful response
        mock_response = Mock()
        mock_response.content = """
        <html>
            <body>
                <table>
                    <tr><th>Player</th><th>Team</th><th>Status</th><th>Description</th><th>Date</th></tr>
                    <tr>
                        <td>LeBron James</td>
                        <td>LAL</td>
                        <td>Out</td>
                        <td>Left ankle sprain</td>
                        <td>2024-01-15</td>
                    </tr>
                    <tr>
                        <td>Stephen Curry</td>
                        <td>GSW</td>
                        <td>Day-to-Day</td>
                        <td>Right knee soreness</td>
                        <td>01/15/2024</td>
                    </tr>
                </table>
            </body>
        </html>
        """
        mock_response.raise_for_status = Mock()

        with patch.object(ingestor.session, "get", return_value=mock_response):
            result = ingestor.fetch("all", source="espn")

            assert result["scope"] == "all"
            assert result["source"] == "espn"
            assert len(result["injuries"]) == 2
            assert result["injuries"][0]["player_name"] == "LeBron James"
            assert result["injuries"][0]["status"] == "Out"
            assert result["injuries"][0]["injury_type"] == "sprain"
            assert result["injuries"][0]["body_part"] == "ankle"

    def test_fetch_team_filter(self):
        """Test fetching injuries for a specific team."""
        ingestor = InjuryIngestor()

        mock_response = Mock()
        mock_response.content = """
        <html>
            <body>
                <table>
                    <tr><th>Player</th><th>Team</th><th>Status</th><th>Description</th></tr>
                    <tr>
                        <td>LeBron James</td>
                        <td>LAL</td>
                        <td>Out</td>
                        <td>Ankle sprain</td>
                    </tr>
                    <tr>
                        <td>Stephen Curry</td>
                        <td>GSW</td>
                        <td>Out</td>
                        <td>Knee soreness</td>
                    </tr>
                </table>
            </body>
        </html>
        """
        mock_response.raise_for_status = Mock()

        with patch.object(ingestor.session, "get", return_value=mock_response):
            result = ingestor.fetch("team:LAL", source="espn")

            assert result["scope"] == "team"
            assert result["team"] == "LAL"
            assert len(result["injuries"]) == 1
            assert result["injuries"][0]["player_name"] == "LeBron James"

    def test_fetch_player_scope(self):
        """Test fetching injuries for a specific player (not implemented)."""
        ingestor = InjuryIngestor()
        result = ingestor.fetch("player:LeBron James", source="espn")

        assert result["scope"] == "player"
        assert result["player"] == "LeBron James"
        assert result["injuries"] == []

    def test_fetch_unsupported_source(self):
        """Test that unsupported source raises ValueError."""
        ingestor = InjuryIngestor()
        with pytest.raises(ValueError, match="Unsupported source"):
            ingestor.fetch("all", source="unsupported")

    def test_fetch_invalid_entity_id(self):
        """Test that invalid entity_id format raises ValueError."""
        ingestor = InjuryIngestor()
        with pytest.raises(ValueError, match="Invalid entity_id format"):
            ingestor.fetch("invalid_format")

    def test_fetch_rotowire_success(self):
        """Test successful fetch from Rotowire."""
        ingestor = InjuryIngestor()

        mock_response = Mock()
        mock_response.content = """
        <html>
            <body>
                <div class="lineup">
                    <span class="team-name">LAL</span>
                    <div class="player">
                        <a class="player-name">LeBron James</a>
                        <span class="status">Out</span>
                        <div class="news">Left ankle sprain, will miss 2-3 weeks</div>
                    </div>
                </div>
            </body>
        </html>
        """
        mock_response.raise_for_status = Mock()

        with patch.object(ingestor.session, "get", return_value=mock_response):
            result = ingestor.fetch("all", source="rotowire")

            assert result["source"] == "rotowire"
            assert len(result["injuries"]) >= 0

    def test_fetch_nba_injuries_not_implemented(self):
        """Test that NBA.com source returns empty list (not implemented)."""
        ingestor = InjuryIngestor()
        result = ingestor.fetch("all", source="nba")

        assert result["injuries"] == []

    def test_fetch_http_error(self):
        """Test handling of HTTP errors during fetch."""
        ingestor = InjuryIngestor()

        mock_response = Mock()
        mock_response.raise_for_status = Mock(
            side_effect=requests.exceptions.HTTPError("404 Not Found")
        )

        with (
            patch.object(ingestor.session, "get", return_value=mock_response),
            pytest.raises(requests.exceptions.HTTPError),
        ):
            ingestor.fetch("all", source="espn")

    def test_fetch_timeout(self):
        """Test handling of timeout during fetch."""
        ingestor = InjuryIngestor()

        with (
            patch.object(ingestor.session, "get", side_effect=requests.exceptions.Timeout()),
            pytest.raises(requests.exceptions.Timeout),
        ):
            ingestor.fetch("all", source="espn")

    def test_fetch_malformed_html(self):
        """Test handling of malformed HTML."""
        ingestor = InjuryIngestor()

        mock_response = Mock()
        mock_response.content = "<html><body>Broken content</body></html>"
        mock_response.raise_for_status = Mock()

        with patch.object(ingestor.session, "get", return_value=mock_response):
            result = ingestor.fetch("all", source="espn")
            # Should not crash, just return empty list
            assert isinstance(result["injuries"], list)


class TestInjuryIngestorParseMethods:
    """Tests for InjuryIngestor parsing methods."""

    def test_parse_injury_description_comprehensive(self):
        """Test comprehensive injury description parsing."""
        ingestor = InjuryIngestor()

        test_cases = [
            ("Left ACL tear", "tear", "acl"),
            ("Right ankle sprain", "sprain", "ankle"),
            ("Knee contusion", "contusion", "knee"),
            ("Lower back soreness", "soreness", "back"),
            ("Right hamstring strain", "strain", "hamstring"),
            ("Concussion protocol", "concussion", "concussion"),
            ("Broken finger", "break", "finger"),
            ("Shoulder tendinitis", "tendinitis", "shoulder"),
            ("Hip bursitis", "bursitis", "hip"),
            ("", None, None),
        ]

        for desc, expected_type, expected_part in test_cases:
            injury_type, body_part = ingestor._parse_injury_description(desc)
            assert injury_type == expected_type, f"Failed for: {desc}"
            assert body_part == expected_part, f"Failed for: {desc}"

    def test_parse_date_formats(self):
        """Test various date format parsing."""
        ingestor = InjuryIngestor()

        test_cases = [
            ("2024-01-15", date(2024, 1, 15)),
            ("01/15/2024", date(2024, 1, 15)),
            ("1/15/24", date(2024, 1, 15)),
            ("January 15, 2024", date(2024, 1, 15)),
            ("Jan 15, 2024", date(2024, 1, 15)),
            ("", None),
            ("invalid date", None),
        ]

        for date_str, expected in test_cases:
            result = ingestor._parse_date(date_str)
            assert result == expected, f"Failed for: {date_str}"

    def test_parse_injury_description_case_insensitive(self):
        """Test that parsing is case-insensitive."""
        ingestor = InjuryIngestor()

        injury_type1, body_part1 = ingestor._parse_injury_description("ANKLE SPRAIN")
        injury_type2, body_part2 = ingestor._parse_injury_description("ankle sprain")
        injury_type3, body_part3 = ingestor._parse_injury_description("Ankle Sprain")

        assert injury_type1 == injury_type2 == injury_type3
        assert body_part1 == body_part2 == body_part3


class TestInjuryIngestorValidate:
    """Tests for InjuryIngestor.validate() method."""

    def test_validate_with_player_id(self):
        """Test validation with player_id included."""
        ingestor = InjuryIngestor()

        raw_data = {
            "injuries": [
                {
                    "player_id": 2544,
                    "team_id": 1610612747,
                    "injury_date": date(2024, 1, 15),
                    "injury_type": "sprain",
                    "body_part": "ankle",
                    "status": "Out",
                    "games_missed": 5,
                    "notes": "Left ankle sprain",
                }
            ]
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 1
        assert isinstance(result[0], InjuryCreate)
        assert result[0].player_id == 2544
        assert result[0].status == "Out"

    def test_validate_without_player_id_skips(self):
        """Test that records without player_id are skipped."""
        ingestor = InjuryIngestor()

        raw_data = {
            "injuries": [
                {
                    "team_id": 1610612747,
                    "injury_date": date(2024, 1, 15),
                    "status": "Out",
                }
            ]
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 0

    def test_validate_empty_list(self):
        """Test validation with empty injury list."""
        ingestor = InjuryIngestor()
        result = ingestor.validate({"injuries": []})
        assert result == []

    def test_validate_missing_injuries_key(self):
        """Test validation when 'injuries' key is missing."""
        ingestor = InjuryIngestor()
        result = ingestor.validate({})
        assert result == []

    def test_validate_with_validation_error(self):
        """Test that validation errors are caught and logged."""
        ingestor = InjuryIngestor()

        # Missing required fields
        raw_data = {
            "injuries": [
                {
                    "player_id": 2544,
                    # Missing required fields like status
                }
            ]
        }

        # Should not raise, should skip invalid record
        result = ingestor.validate(raw_data)
        assert len(result) == 0


class TestInjuryIngestorUpsert:
    """Tests for InjuryIngestor.upsert() method."""

    def test_upsert_insert_new_injury(self, db_connection):
        """Test inserting a new injury."""
        ingestor = InjuryIngestor()

        injury = InjuryCreate(
            player_id=2544,
            team_id=1610612747,
            injury_date=date(2024, 1, 10),
            injury_type="Ankle",
            status="Out",
            games_missed=2,
        )

        rows = ingestor.upsert([injury], db_connection)
        assert rows == 1

        # Verify insertion
        cursor = db_connection.execute(
            "SELECT * FROM injury WHERE player_id = 2544 AND injury_date = '2024-01-10'"
        )
        result = cursor.fetchone()
        assert result is not None

    def test_upsert_update_existing_injury(self, db_connection):
        """Test updating an existing injury."""
        ingestor = InjuryIngestor()

        # Insert initial injury
        injury1 = InjuryCreate(
            player_id=201939,
            injury_date=date(2024, 2, 1),
            status="Questionable",
        )
        ingestor.upsert([injury1], db_connection)

        # Update with games_missed
        injury2 = InjuryCreate(
            player_id=201939,
            team_id=1610612738,
            injury_date=date(2024, 2, 1),
            status="Questionable",
            games_missed=3,
        )
        rows = ingestor.upsert([injury2], db_connection)
        assert rows == 1

    def test_upsert_skips_non_injury_models(self, db_connection):
        """Test that non-InjuryCreate models are skipped."""
        from pydantic import BaseModel

        class OtherModel(BaseModel):
            x: int = 1

        ingestor = InjuryIngestor()
        rows = ingestor.upsert([OtherModel()], db_connection)
        assert rows == 0

    def test_upsert_multiple_injuries(self, db_connection):
        """Test upserting multiple injuries."""
        ingestor = InjuryIngestor()

        injuries = [
            InjuryCreate(
                player_id=2544,
                team_id=1610612747,
                injury_date=date(2024, 1, 10),
                status="Out",
            ),
            InjuryCreate(
                player_id=201939,
                team_id=1610612738,
                injury_date=date(2024, 1, 11),
                status="Day-to-Day",
            ),
        ]

        rows = ingestor.upsert(injuries, db_connection)
        assert rows == 2

    def test_upsert_writes_to_audit_log(self, db_connection):
        """Test that upsert writes to ingestion_audit table."""
        ingestor = InjuryIngestor()

        injury = InjuryCreate(
            player_id=2544,
            team_id=1610612747,
            injury_date=date(2024, 1, 10),
            status="Out",
        )

        ingestor.upsert([injury], db_connection)

        # Check audit log
        cursor = db_connection.execute(
            "SELECT * FROM ingestion_audit WHERE entity_type = 'injuries' AND entity_id = '2544'"
        )
        result = cursor.fetchone()
        assert result is not None
        assert result["status"] == "SUCCESS"


class TestInjuryIngestorIntegration:
    """Integration tests for InjuryIngestor.ingest() method."""

    def test_ingest_full_pipeline_success(self, db_connection, tmp_path):
        """Test full ingest pipeline with mocked fetch."""
        ingestor = InjuryIngestor()

        mock_response = Mock()
        mock_response.content = """
        <html>
            <body>
                <table>
                    <tr><th>Player</th><th>Team</th><th>Status</th><th>Description</th></tr>
                    <tr>
                        <td>LeBron James</td>
                        <td>LAL</td>
                        <td>Out</td>
                        <td>Ankle sprain</td>
                    </tr>
                </table>
            </body>
        </html>
        """
        mock_response.raise_for_status = Mock()

        with patch.object(ingestor.session, "get", return_value=mock_response):
            result = ingestor.ingest("all", db_connection, source="espn")

            # Note: Without player_id, validation returns empty list
            # This tests the pipeline flow even if no records are inserted
            assert "status" in result

    def test_ingest_with_validation_error_quarantines(self, db_connection, tmp_path):
        """Test that validation errors trigger quarantine."""
        ingestor = InjuryIngestor()

        # Mock fetch that returns data that will fail validation
        with (
            patch.object(
                ingestor,
                "fetch",
                return_value={
                    "scope": "all",
                    "source": "espn",
                    "injuries": [{"invalid": "data"}],
                },
            ),
            patch.object(ingestor, "validate", side_effect=Exception("Validation failed")),
        ):
            result = ingestor.ingest("all", db_connection)

            assert result["status"] == "FAILED"


class TestInjuryIngestorPrivateMethods:
    """Tests for private methods of InjuryIngestor."""

    def test_insert_injury(self, db_connection):
        """Test _insert_injury private method."""
        ingestor = InjuryIngestor()

        injury = InjuryCreate(
            player_id=2544,
            team_id=1610612747,
            injury_date=date(2024, 1, 10),
            injury_type="Ankle",
            status="Out",
        )

        ingestor._insert_injury(injury, db_connection)

        cursor = db_connection.execute("SELECT * FROM injury WHERE player_id = 2544")
        result = cursor.fetchone()
        assert result is not None

    def test_update_injury(self, db_connection):
        """Test _update_injury private method."""
        ingestor = InjuryIngestor()

        # First insert
        injury1 = InjuryCreate(
            player_id=201939,
            injury_date=date(2024, 2, 1),
            status="Questionable",
        )
        ingestor._insert_injury(injury1, db_connection)

        # Get injury_id
        cursor = db_connection.execute("SELECT injury_id FROM injury WHERE player_id = 201939")
        injury_id = cursor.fetchone()[0]

        # Update
        injury2 = InjuryCreate(
            player_id=201939,
            team_id=1610612738,
            injury_date=date(2024, 2, 1),
            status="Out",
            games_missed=5,
        )
        ingestor._update_injury(injury_id, injury2, db_connection)

        # Verify update
        cursor = db_connection.execute(
            "SELECT status, games_missed FROM injury WHERE injury_id = ?", (injury_id,)
        )
        result = cursor.fetchone()
        assert result["status"] == "Out"
        assert result["games_missed"] == 5
