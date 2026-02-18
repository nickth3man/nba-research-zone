"""Comprehensive tests for ContractIngestor.

Tests cover error paths, edge cases, and web scraping logic.
"""

from unittest.mock import Mock, patch

import pytest
import requests

from nba_vault.ingestion.contracts import ContractIngestor
from nba_vault.models.advanced_stats import PlayerContractCreate


class TestContractIngestorFetch:
    """Tests for ContractIngestor.fetch() method."""

    def test_fetch_all_realgm_not_implemented(self):
        """Test that RealGM all-contracts returns empty (not implemented)."""
        ingestor = ContractIngestor()
        result = ingestor.fetch("all", source="realgm")

        assert result["scope"] == "all"
        assert result["source"] == "realgm"
        assert result["contracts"] == []

    def test_fetch_all_spotrac_success(self):
        """Test successful fetch of all contracts from Spotrac."""
        ingestor = ContractIngestor()

        mock_response = Mock()
        mock_response.content = """
        <html>
            <body>
                <table class="players-table">
                    <tr><th>Player</th><th>Team</th><th>Contract</th></tr>
                    <tr>
                        <td><a href="/nba/players/lebron-james/2544/">LeBron James</a></td>
                        <td>LAL</td>
                        <td>$47,600,000</td>
                    </tr>
                    <tr>
                        <td><a href="/nba/players/stephen-curry/201939/">Stephen Curry</a></td>
                        <td>GSW</td>
                        <td>$51,900,000</td>
                    </tr>
                </table>
            </body>
        </html>
        """
        mock_response.raise_for_status = Mock()

        with patch.object(ingestor.session, "get", return_value=mock_response):
            result = ingestor.fetch("all", source="spotrac")

            assert result["scope"] == "all"
            assert result["source"] == "spotrac"
            assert len(result["contracts"]) == 2
            assert result["contracts"][0]["player_name"] == "LeBron James"
            assert result["contracts"][0]["player_url"] == "/nba/players/lebron-james/2544/"

    def test_fetch_team_realgm_not_implemented(self):
        """Test that RealGM team contracts returns empty (not implemented)."""
        ingestor = ContractIngestor()
        result = ingestor.fetch("team:LAL", source="realgm")

        assert result["scope"] == "team"
        assert result["team"] == "LAL"
        assert result["contracts"] == []

    def test_fetch_team_spotrac_not_implemented(self):
        """Test that Spotrac team contracts returns empty (not implemented)."""
        ingestor = ContractIngestor()
        result = ingestor.fetch("team:LAL", source="spotrac")

        assert result["scope"] == "team"
        assert result["team"] == "LAL"
        assert result["contracts"] == []

    def test_fetch_player_realgm_not_implemented(self):
        """Test that RealGM player contracts returns empty (not implemented)."""
        ingestor = ContractIngestor()
        result = ingestor.fetch("player:2544", source="realgm")

        assert result["scope"] == "player"
        assert result["player"] == "2544"
        assert result["contracts"] == []

    def test_fetch_player_spotrac_not_implemented(self):
        """Test that Spotrac player contracts returns empty (not implemented)."""
        ingestor = ContractIngestor()
        result = ingestor.fetch("player:2544", source="spotrac")

        assert result["scope"] == "player"
        assert result["player"] == "2544"
        assert result["contracts"] == []

    def test_fetch_unsupported_source(self):
        """Test that unsupported source raises ValueError."""
        ingestor = ContractIngestor()
        with pytest.raises(ValueError, match="Unsupported source"):
            ingestor.fetch("all", source="unsupported")

    def test_fetch_invalid_entity_id(self):
        """Test that invalid entity_id format raises ValueError."""
        ingestor = ContractIngestor()
        with pytest.raises(ValueError, match="Invalid entity_id format"):
            ingestor.fetch("invalid_format")

    def test_fetch_spotrac_no_table(self):
        """Test handling when Spotrac response has no table."""
        ingestor = ContractIngestor()

        mock_response = Mock()
        mock_response.content = "<html><body>No contracts found</body></html>"
        mock_response.raise_for_status = Mock()

        with patch.object(ingestor.session, "get", return_value=mock_response):
            result = ingestor.fetch("all", source="spotrac")
            assert result["contracts"] == []

    def test_fetch_spotrac_http_error(self):
        """Test handling of HTTP errors during Spotrac fetch."""
        ingestor = ContractIngestor()

        mock_response = Mock()
        mock_response.raise_for_status = Mock(
            side_effect=requests.exceptions.HTTPError("404 Not Found")
        )

        with (
            patch.object(ingestor.session, "get", return_value=mock_response),
            pytest.raises(requests.exceptions.HTTPError),
        ):
            ingestor.fetch("all", source="spotrac")


class TestContractIngestorValidate:
    """Tests for ContractIngestor.validate() method."""

    def test_validate_with_complete_data(self):
        """Test validation with complete contract data."""
        ingestor = ContractIngestor()

        raw_data = {
            "contracts": [
                {
                    "player_id": 2544,
                    "team_id": 1610612747,
                    "season_start": 2023,
                    "season_end": 2026,
                    "salary_amount": 50_000_000.0,
                    "contract_type": "Veteran",
                    "player_option": 1,
                    "team_option": 0,
                    "early_termination": 0,
                    "guaranteed_money": 50_000_000.0,
                    "cap_hit": 47_600_000.0,
                }
            ]
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 1
        assert isinstance(result[0], PlayerContractCreate)
        assert result[0].player_id == 2544
        assert result[0].salary_amount == 50_000_000.0

    def test_validate_without_player_id_skips(self):
        """Test that records without player_id are skipped."""
        ingestor = ContractIngestor()

        raw_data = {
            "contracts": [
                {
                    "team_id": 1610612747,
                    "season_start": 2023,
                    "season_end": 2026,
                }
            ]
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 0

    def test_validate_without_team_id_skips(self):
        """Test that records without team_id are skipped."""
        ingestor = ContractIngestor()

        raw_data = {
            "contracts": [
                {
                    "player_id": 2544,
                    "season_start": 2023,
                    "season_end": 2026,
                }
            ]
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 0

    def test_validate_empty_list(self):
        """Test validation with empty contract list."""
        ingestor = ContractIngestor()
        result = ingestor.validate({"contracts": []})
        assert result == []

    def test_validate_missing_contracts_key(self):
        """Test validation when 'contracts' key is missing."""
        ingestor = ContractIngestor()
        result = ingestor.validate({})
        assert result == []

    def test_validate_with_defaults(self):
        """Test validation with default values."""
        ingestor = ContractIngestor()

        raw_data = {
            "contracts": [
                {
                    "player_id": 2544,
                    "team_id": 1610612747,
                    "season_start": 2023,
                    "season_end": 2026,
                }
            ]
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 1
        assert result[0].player_option == 0  # type: ignore[attr-defined]
        assert result[0].team_option == 0  # type: ignore[attr-defined]
        assert result[0].early_termination == 0  # type: ignore[attr-defined]

    def test_validate_multiple_contracts(self):
        """Test validation of multiple contracts."""
        ingestor = ContractIngestor()

        raw_data = {
            "contracts": [
                {
                    "player_id": 2544,
                    "team_id": 1610612747,
                    "season_start": 2023,
                    "season_end": 2026,
                },
                {
                    "player_id": 201939,
                    "team_id": 1610612738,
                    "season_start": 2023,
                    "season_end": 2025,
                },
            ]
        }

        result = ingestor.validate(raw_data)
        assert len(result) == 2


class TestContractIngestorUpsert:
    """Tests for ContractIngestor.upsert() method."""

    def test_upsert_insert_new_contract(self, db_connection):
        """Test inserting a new contract."""
        ingestor = ContractIngestor()

        contract = PlayerContractCreate(
            player_id=2544,
            team_id=1610612747,
            season_start=2023,
            season_end=2026,
            salary_amount=50_000_000.0,
            contract_type="Veteran",
        )

        rows = ingestor.upsert([contract], db_connection)
        assert rows == 1

        cursor = db_connection.execute(
            "SELECT salary_amount FROM player_contract "
            "WHERE player_id = 2544 AND season_start = 2023"
        )
        result = cursor.fetchone()
        assert result is not None
        assert result[0] == 50_000_000.0

    def test_upsert_update_existing_contract(self, db_connection):
        """Test updating an existing contract."""
        ingestor = ContractIngestor()

        # Insert initial contract
        contract1 = PlayerContractCreate(
            player_id=201939,
            team_id=1610612738,
            season_start=2023,
            season_end=2025,
            salary_amount=30_000_000.0,
        )
        ingestor.upsert([contract1], db_connection)

        # Update with different values
        contract2 = PlayerContractCreate(
            player_id=201939,
            team_id=1610612738,
            season_start=2023,
            season_end=2026,  # Changed
            salary_amount=32_000_000.0,  # Changed
        )
        rows = ingestor.upsert([contract2], db_connection)
        assert rows == 1

        # Verify update
        cursor = db_connection.execute(
            "SELECT season_end, salary_amount FROM player_contract "
            "WHERE player_id = 201939 AND season_start = 2023"
        )
        result = cursor.fetchone()
        assert result[0] == 2026
        assert result[1] == 32_000_000.0

    def test_upsert_skips_non_contract_models(self, db_connection):
        """Test that non-PlayerContractCreate models are skipped."""
        from pydantic import BaseModel

        class OtherModel(BaseModel):
            x: int = 1

        ingestor = ContractIngestor()
        rows = ingestor.upsert([OtherModel()], db_connection)
        assert rows == 0

    def test_upsert_multiple_contracts(self, db_connection):
        """Test upserting multiple contracts."""
        ingestor = ContractIngestor()

        contracts = [
            PlayerContractCreate(
                player_id=2544,
                team_id=1610612747,
                season_start=2023,
                season_end=2026,
            ),
            PlayerContractCreate(
                player_id=201939,
                team_id=1610612738,
                season_start=2023,
                season_end=2025,
            ),
        ]

        rows = ingestor.upsert(contracts, db_connection)
        assert rows == 2

    def test_upsert_writes_to_audit_log(self, db_connection):
        """Test that upsert writes to ingestion_audit table."""
        ingestor = ContractIngestor()

        contract = PlayerContractCreate(
            player_id=2544,
            team_id=1610612747,
            season_start=2023,
            season_end=2026,
        )

        ingestor.upsert([contract], db_connection)

        # Check audit log
        cursor = db_connection.execute(
            "SELECT * FROM ingestion_audit WHERE entity_type = 'contracts' AND entity_id = '2544'"
        )
        result = cursor.fetchone()
        assert result is not None
        assert result["status"] == "SUCCESS"

    def test_upsert_contract_with_options(self, db_connection):
        """Test upserting contract with various options."""
        ingestor = ContractIngestor()

        contract = PlayerContractCreate(
            player_id=2544,
            team_id=1610612747,
            season_start=2023,
            season_end=2026,
            player_option=1,
            team_option=0,
            early_termination=1,
            guaranteed_money=150_000_000.0,
        )

        rows = ingestor.upsert([contract], db_connection)
        assert rows == 1

        # Verify
        cursor = db_connection.execute(
            "SELECT player_option, team_option, early_termination, guaranteed_money "
            "FROM player_contract WHERE player_id = 2544 AND season_start = 2023"
        )
        result = cursor.fetchone()
        assert result[0] == 1
        assert result[1] == 0
        assert result[2] == 1
        assert result[3] == 150_000_000.0


class TestContractIngestorIntegration:
    """Integration tests for ContractIngestor.ingest() method."""

    def test_ingest_full_pipeline_success(self, db_connection):
        """Test full ingest pipeline with mocked fetch."""
        ingestor = ContractIngestor()

        with patch.object(
            ingestor,
            "fetch",
            return_value={
                "scope": "all",
                "source": "realgm",
                "contracts": [],
            },
        ):
            result = ingestor.ingest("all", db_connection)
            assert result["status"] == "SUCCESS"

    def test_ingest_with_validation_error(self, db_connection):
        """Test that validation errors are handled."""
        ingestor = ContractIngestor()

        with patch.object(
            ingestor,
            "fetch",
            return_value={
                "scope": "all",
                "source": "realgm",
                "contracts": [{"invalid": "data"}],
            },
        ):
            # Should handle gracefully
            result = ingestor.ingest("all", db_connection)
            assert "status" in result


class TestContractIngestorPrivateMethods:
    """Tests for private methods of ContractIngestor."""

    def test_insert_contract(self, db_connection):
        """Test _insert_contract private method."""
        ingestor = ContractIngestor()

        contract = PlayerContractCreate(
            player_id=2544,
            team_id=1610612747,
            season_start=2023,
            season_end=2026,
            salary_amount=50_000_000.0,
        )

        ingestor._insert_contract(contract, db_connection)

        cursor = db_connection.execute(
            "SELECT * FROM player_contract WHERE player_id = 2544 AND season_start = 2023"
        )
        result = cursor.fetchone()
        assert result is not None

    def test_update_contract(self, db_connection):
        """Test _update_contract private method."""
        ingestor = ContractIngestor()

        # First insert
        contract1 = PlayerContractCreate(
            player_id=201939,
            team_id=1610612738,
            season_start=2023,
            season_end=2025,
            salary_amount=30_000_000.0,
        )
        ingestor._insert_contract(contract1, db_connection)

        # Update
        contract2 = PlayerContractCreate(
            player_id=201939,
            team_id=1610612738,
            season_start=2023,
            season_end=2026,
            salary_amount=35_000_000.0,
            contract_type="Supermax",
        )
        ingestor._update_contract(contract2, db_connection)

        # Verify update
        cursor = db_connection.execute(
            "SELECT season_end, salary_amount, contract_type "
            "FROM player_contract WHERE player_id = 201939 AND season_start = 2023"
        )
        result = cursor.fetchone()
        assert result[0] == 2026
        assert result[1] == 35_000_000.0
        assert result[2] == "Supermax"


class TestContractIngestorEdgeCases:
    """Edge case tests for ContractIngestor."""

    def test_max_salary_amount(self, db_connection):
        """Test handling of maximum salary amounts."""
        ingestor = ContractIngestor()

        contract = PlayerContractCreate(
            player_id=2544,
            team_id=1610612747,
            season_start=2023,
            season_end=2026,
            salary_amount=999_999_999.99,
        )

        rows = ingestor.upsert([contract], db_connection)
        assert rows == 1

    def test_zero_salary_amount(self, db_connection):
        """Test handling of zero salary (minimum contracts)."""
        ingestor = ContractIngestor()

        contract = PlayerContractCreate(
            player_id=2544,
            team_id=1610612747,
            season_start=2023,
            season_end=2024,
            salary_amount=0.0,
        )

        rows = ingestor.upsert([contract], db_connection)
        assert rows == 1

    def test_long_contract_duration(self, db_connection):
        """Test handling of long contract durations (5+ years)."""
        ingestor = ContractIngestor()

        contract = PlayerContractCreate(
            player_id=2544,
            team_id=1610612747,
            season_start=2023,
            season_end=2030,  # 7-year contract
        )

        rows = ingestor.upsert([contract], db_connection)
        assert rows == 1

    def test_same_player_multiple_teams(self, db_connection):
        """Test same player with contracts for different teams (trades/signings)."""
        ingestor = ContractIngestor()

        contracts = [
            PlayerContractCreate(
                player_id=2544,
                team_id=1610612747,  # LAL
                season_start=2023,
                season_end=2024,
            ),
            PlayerContractCreate(
                player_id=2544,
                team_id=1610612738,  # GSW
                season_start=2024,
                season_end=2027,
            ),
        ]

        rows = ingestor.upsert(contracts, db_connection)
        assert rows == 2

        # Verify both exist
        cursor = db_connection.execute(
            "SELECT team_id FROM player_contract WHERE player_id = 2544 ORDER BY season_start"
        )
        results = cursor.fetchall()
        assert len(results) == 2
        assert results[0][0] == 1610612747
        assert results[1][0] == 1610612738
