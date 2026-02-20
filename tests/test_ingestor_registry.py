"""Tests for ingestor registry."""

from nba_vault.ingestion.contracts import ContractIngestor
from nba_vault.ingestion.injuries import InjuryIngestor
from nba_vault.ingestion.lineups import LineupsIngestor
from nba_vault.ingestion.player_tracking import PlayerTrackingIngestor
from nba_vault.ingestion.team_advanced_stats import TeamAdvancedStatsIngestor
from nba_vault.ingestion.team_other_stats import TeamOtherStatsIngestor


class TestIngestorRegistry:
    """Tests for ingestor registry."""

    def test_all_new_ingestors_registered(self):
        """Test that all new ingestors are registered."""
        from nba_vault.ingestion import list_ingestors

        # Check that all new ingestors are in the registry
        registered_types = list_ingestors()

        assert "player_tracking" in registered_types
        assert "lineups" in registered_types
        assert "team_other_stats" in registered_types
        assert "team_advanced_stats" in registered_types
        assert "injuries" in registered_types
        assert "contracts" in registered_types

    def test_create_ingestor_instances(self):
        """Test creating ingestor instances."""
        from nba_vault.ingestion import create_ingestor

        player_tracking = create_ingestor("player_tracking")
        assert player_tracking is not None
        assert isinstance(player_tracking, PlayerTrackingIngestor)

        lineups = create_ingestor("lineups")
        assert lineups is not None
        assert isinstance(lineups, LineupsIngestor)

        team_other = create_ingestor("team_other_stats")
        assert team_other is not None
        assert isinstance(team_other, TeamOtherStatsIngestor)

        team_advanced = create_ingestor("team_advanced_stats")
        assert team_advanced is not None
        assert isinstance(team_advanced, TeamAdvancedStatsIngestor)

        injuries = create_ingestor("injuries")
        assert injuries is not None
        assert isinstance(injuries, InjuryIngestor)

        contracts = create_ingestor("contracts")
        assert contracts is not None
        assert isinstance(contracts, ContractIngestor)
