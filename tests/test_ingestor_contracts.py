"""Tests for ContractIngestor."""

import pytest

from nba_vault.ingestion.contracts import ContractIngestor
from nba_vault.models.advanced_stats import PlayerContractCreate


class TestContractIngestor:
    """Tests for ContractIngestor (intentional stub — raises NotImplementedError)."""

    def test_init(self):
        """Test ingestor initialization."""
        ingestor = ContractIngestor()
        assert ingestor.entity_type == "contracts"

    def test_fetch_raises_not_implemented(self):
        """Contract fetch is intentionally not implemented."""
        ingestor = ContractIngestor()
        with pytest.raises(NotImplementedError):
            ingestor.fetch("all", source="realgm")

    def test_validate_raises_not_implemented(self):
        """Contract validate is intentionally not implemented."""
        ingestor = ContractIngestor()
        with pytest.raises(NotImplementedError):
            ingestor.validate({})


class TestContractUpsert:
    """Tests for ContractIngestor.upsert() (intentional stub — raises NotImplementedError)."""

    def test_upsert_raises_not_implemented(self, db_connection):
        """Contract upsert is intentionally not implemented."""

        ingestor = ContractIngestor()
        models = [
            PlayerContractCreate(
                player_id=2544,
                team_id=1610612747,
                season_start=2023,
                season_end=2025,
                salary_amount=46_000_000.0,
                contract_type="Veteran",
            )
        ]
        with pytest.raises(NotImplementedError):
            ingestor.upsert(models, db_connection)
