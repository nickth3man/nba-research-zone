"""Tests for ContractIngestor stub (salary data excluded per PRD §3).

All three pipeline methods (fetch, validate, upsert) must raise
NotImplementedError — contracts are intentionally not sourced.
"""

import pytest

from nba_vault.ingestion.contracts import ContractIngestor


class TestContractIngestorIsStub:
    """Verify that ContractIngestor raises NotImplementedError on all paths."""

    def test_fetch_raises_not_implemented(self) -> None:
        """fetch() must raise NotImplementedError."""
        ingestor = ContractIngestor()
        with pytest.raises(NotImplementedError):
            ingestor.fetch("all")

    def test_validate_raises_not_implemented(self) -> None:
        """validate() must raise NotImplementedError."""
        ingestor = ContractIngestor()
        with pytest.raises(NotImplementedError):
            ingestor.validate({})

    def test_upsert_raises_not_implemented(self) -> None:
        """upsert() must raise NotImplementedError."""
        ingestor = ContractIngestor()
        with pytest.raises(NotImplementedError):
            ingestor.upsert([], None)  # type: ignore[arg-type]

    def test_ingest_raises_not_implemented(self, db_connection) -> None:  # type: ignore[no-untyped-def]
        """ingest() delegates to fetch(), which raises NotImplementedError."""
        ingestor = ContractIngestor()
        with pytest.raises(NotImplementedError):
            ingestor.ingest("all", db_connection)

    def test_entity_type_is_contracts(self) -> None:
        """entity_type must remain 'contracts' so registry still resolves it."""
        ingestor = ContractIngestor()
        assert ingestor.entity_type == "contracts"
