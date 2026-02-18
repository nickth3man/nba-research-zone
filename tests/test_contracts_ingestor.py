"""Tests for ContractIngestor stub (salary data excluded per PRD §3).

All three pipeline methods (fetch, validate, upsert) must raise
NotImplementedError — contracts are intentionally not sourced.
"""

from unittest.mock import Mock, patch

import pytest

from nba_vault.ingestion.contracts import ContractIngestor


def _fast_retry_settings():
    s = Mock()
    s.nba_api_retry_attempts = 1
    s.nba_api_retry_delay = 0
    return s


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

    def test_ingest_returns_failed_status(self, db_connection) -> None:  # type: ignore[no-untyped-def]
        """ingest() catches NotImplementedError from fetch() and returns FAILED status."""
        ingestor = ContractIngestor()
        with patch("nba_vault.utils.rate_limit.get_settings", return_value=_fast_retry_settings()):
            result = ingestor.ingest("all", db_connection)
        assert result["status"] == "FAILED"
        assert result["error"] == "NotImplementedError"

    def test_entity_type_is_contracts(self) -> None:
        """entity_type must remain 'contracts' so registry still resolves it."""
        ingestor = ContractIngestor()
        assert ingestor.entity_type == "contracts"
