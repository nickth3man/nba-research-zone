"""Pydantic models for data validation."""

from nba_vault.models.advanced_stats import (
    DraftCombineCreate,
    InjuryCreate,
    LineupCreate,
    LineupGameLogCreate,
    PlayerContractCreate,
    PlayerGameMiscStatsCreate,
    PlayerGameTrackingCreate,
    PlayerSeasonMetadataCreate,
    PossessionCreate,
    TeamGameOtherStatsCreate,
    TeamSeasonAdvancedCreate,
)
from nba_vault.models.coach import Coach, CoachCreate
from nba_vault.models.entities import (
    ArenaCreate,
    AwardCreate,
    BoxScoreAdvancedRowCreate,
    BoxScoreHustleRowCreate,
    BoxScorePlayerRowCreate,
    BoxScoreTeamRowCreate,
    CoachStintCreate,
    DraftCombineAnthroCreate,
    DraftPickCreate,
    FranchiseCreate,
    GameCreate,
    GameOfficialCreate,
    PlayByPlayEventCreate,
    PlayerBioCreate,
    PlayerIdXrefCreate,
    PlayerSeasonStatsCreate,
    PlayoffSeriesCreate,
    ShotChartRowCreate,
    TransactionCreate,
)
from nba_vault.models.entities import (
    CoachCreate as CoachEntityCreate,
)
from nba_vault.models.entities import (
    OfficialCreate as OfficialEntityCreate,
)
from nba_vault.models.entities import (
    SeasonCreate as SeasonEntityCreate,
)
from nba_vault.models.entities import (
    TeamCreate as TeamEntityCreate,
)
from nba_vault.models.franchise import Franchise
from nba_vault.models.franchise import FranchiseCreate as FranchiseModelCreate
from nba_vault.models.game import Game
from nba_vault.models.game import GameCreate as GameModelCreate
from nba_vault.models.league import League, LeagueCreate
from nba_vault.models.official import Official, OfficialCreate
from nba_vault.models.player import Player, PlayerCreate
from nba_vault.models.season import Season, SeasonCreate
from nba_vault.models.team import Team, TeamCreate

__all__ = [
    # entities models
    "ArenaCreate",
    "AwardCreate",
    "BoxScoreAdvancedRowCreate",
    "BoxScoreHustleRowCreate",
    "BoxScorePlayerRowCreate",
    "BoxScoreTeamRowCreate",
    # advanced_stats models
    "Coach",
    "CoachCreate",
    "CoachEntityCreate",
    "CoachStintCreate",
    "DraftCombineAnthroCreate",
    "DraftCombineCreate",
    "DraftPickCreate",
    "Franchise",
    "FranchiseCreate",
    "FranchiseCreate",
    "FranchiseModelCreate",
    "Game",
    "GameCreate",
    "GameCreate",
    "GameModelCreate",
    "GameOfficialCreate",
    "InjuryCreate",
    "League",
    "LeagueCreate",
    "LineupCreate",
    "LineupGameLogCreate",
    "Official",
    "OfficialCreate",
    "OfficialEntityCreate",
    "PlayByPlayEventCreate",
    "Player",
    "PlayerBioCreate",
    "PlayerContractCreate",
    "PlayerCreate",
    "PlayerGameMiscStatsCreate",
    "PlayerGameTrackingCreate",
    "PlayerIdXrefCreate",
    "PlayerSeasonMetadataCreate",
    "PlayerSeasonStatsCreate",
    "PlayoffSeriesCreate",
    "PossessionCreate",
    "Season",
    "SeasonCreate",
    "SeasonEntityCreate",
    "ShotChartRowCreate",
    "Team",
    "TeamCreate",
    "TeamEntityCreate",
    "TeamGameOtherStatsCreate",
    "TeamSeasonAdvancedCreate",
    "TransactionCreate",
]
