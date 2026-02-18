"""Data ingestion framework."""

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.registry import get_ingestor, register_ingestor
from nba_vault.ingestion.players import PlayersIngestor

__all__ = ["BaseIngestor", "get_ingestor", "register_ingestor", "PlayersIngestor"]
