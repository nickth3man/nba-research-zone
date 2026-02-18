"""Data ingestion framework.

All ingestors are imported here to trigger @register_ingestor decoration
and ensure auto-discovery via list_ingestors() / create_ingestor().
"""

from nba_vault.ingestion.awards import AwardsIngestor
from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.box_scores import BoxScoreTraditionalIngestor
from nba_vault.ingestion.box_scores_advanced import BoxScoreAdvancedIngestor
from nba_vault.ingestion.box_scores_hustle import BoxScoreHustleIngestor
from nba_vault.ingestion.coaches import CoachIngestor
from nba_vault.ingestion.contracts import ContractIngestor
from nba_vault.ingestion.draft import DraftIngestor
from nba_vault.ingestion.draft_combine import DraftCombineIngestor
from nba_vault.ingestion.games import GameOfficialIngestor, GameScheduleIngestor
from nba_vault.ingestion.injuries import InjuryIngestor
from nba_vault.ingestion.lineups import LineupsIngestor
from nba_vault.ingestion.nba_stats_client import NBAStatsClient
from nba_vault.ingestion.play_by_play import PlayByPlayIngestor
from nba_vault.ingestion.player_bio import PlayerBioIngestor
from nba_vault.ingestion.player_season_stats import PlayerSeasonStatsIngestor
from nba_vault.ingestion.player_tracking import PlayerTrackingIngestor
from nba_vault.ingestion.players import PlayersIngestor
from nba_vault.ingestion.registry import (
    create_ingestor,
    get_ingestor,
    list_ingestors,
    register_ingestor,
)
from nba_vault.ingestion.seasons import FranchiseIngestor, SeasonIngestor
from nba_vault.ingestion.shot_chart import ShotChartIngestor
from nba_vault.ingestion.team_advanced_stats import TeamAdvancedStatsIngestor
from nba_vault.ingestion.team_other_stats import TeamOtherStatsIngestor

__all__ = [
    "AwardsIngestor",
    "BaseIngestor",
    "BoxScoreAdvancedIngestor",
    "BoxScoreHustleIngestor",
    "BoxScoreTraditionalIngestor",
    "CoachIngestor",
    "ContractIngestor",
    "DraftCombineIngestor",
    "DraftIngestor",
    "FranchiseIngestor",
    "GameOfficialIngestor",
    "GameScheduleIngestor",
    "InjuryIngestor",
    "LineupsIngestor",
    "NBAStatsClient",
    "PlayByPlayIngestor",
    "PlayerBioIngestor",
    "PlayerSeasonStatsIngestor",
    "PlayerTrackingIngestor",
    "PlayersIngestor",
    "SeasonIngestor",
    "ShotChartIngestor",
    "TeamAdvancedStatsIngestor",
    "TeamOtherStatsIngestor",
    "create_ingestor",
    "get_ingestor",
    "list_ingestors",
    "register_ingestor",
]
