"""Player contract data ingestor — intentionally not implemented.

Per PRD §3 (Data Sourcing Decisions), salary / contract data is explicitly
excluded from NBA Vault because:
  1. No freely-available, legally-unambiguous API exists for historical salary data.
  2. Sites like Spotrac and RealGM prohibit automated scraping in their ToS.
  3. Salary cap figures change frequently and retroactive corrections are common.

If you need salary data, source it manually from HoopsHype or Basketball-Reference
and import it via a one-off migration script (see migrations/0002_seed_data.sql
for an example of seed-based imports).

This stub is retained in the registry so that existing tests and CLI commands
that reference ContractIngestor continue to fail gracefully rather than
raising an ImportError.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.registry import register_ingestor

if TYPE_CHECKING:
    import pydantic

logger = structlog.get_logger(__name__)


@register_ingestor
class ContractIngestor(BaseIngestor):
    """
    Stub ingestor — contract / salary data intentionally excluded (see module docstring).

    All three pipeline methods raise NotImplementedError immediately.
    """

    entity_type = "contracts"

    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        raise NotImplementedError(
            "Contract ingestion is intentionally not implemented. "
            "See nba_vault/ingestion/contracts.py module docstring for rationale."
        )

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        raise NotImplementedError("Contract ingestion is intentionally not implemented.")

    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        raise NotImplementedError("Contract ingestion is intentionally not implemented.")
