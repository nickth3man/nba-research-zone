"""Data ingestion framework."""

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.contracts import ContractIngestor
from nba_vault.ingestion.injuries import InjuryIngestor
from nba_vault.ingestion.lineups import LineupsIngestor
from nba_vault.ingestion.nba_stats_client import NBAStatsClient
from nba_vault.ingestion.player_tracking import PlayerTrackingIngestor
from nba_vault.ingestion.players import PlayersIngestor
from nba_vault.ingestion.registry import get_ingestor, register_ingestor
from nba_vault.ingestion.team_advanced_stats import TeamAdvancedStatsIngestor
from nba_vault.ingestion.team_other_stats import TeamOtherStatsIngestor

__all__ = [
    "BaseIngestor",
    "ContractIngestor",
    "InjuryIngestor",
    "LineupsIngestor",
    "NBAStatsClient",
    "PlayerTrackingIngestor",
    "PlayersIngestor",
    "TeamAdvancedStatsIngestor",
    "TeamOtherStatsIngestor",
    "get_ingestor",
    "register_ingestor",
]
