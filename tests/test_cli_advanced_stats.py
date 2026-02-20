"""Tests for advanced-stats CLI commands."""

from unittest.mock import patch

from typer.testing import CliRunner

from nba_vault.cli import app

runner = CliRunner()


def test_advanced_stats_tracking_by_player(patch_db_connection, patch_create_ingestor):
    result = runner.invoke(app, ["advanced-stats", "ingest-tracking", "--player-id", "2544"])

    assert result.exit_code == 0
    assert "Successfully ingested" in result.output


def test_advanced_stats_tracking_by_team(patch_db_connection, patch_create_ingestor):
    result = runner.invoke(app, ["advanced-stats", "ingest-tracking", "--team-id", "1610612747"])

    assert result.exit_code == 0


def test_advanced_stats_tracking_no_id(patch_db_connection, patch_create_ingestor):
    result = runner.invoke(app, ["advanced-stats", "ingest-tracking"])

    assert result.exit_code == 1
    assert "Must specify" in result.output


def test_advanced_stats_tracking_failure(patch_db_connection, mock_ingestor, patch_create_ingestor):
    patch_create_ingestor.return_value = mock_ingestor(status="FAILED", error_message="API error")

    result = runner.invoke(app, ["advanced-stats", "ingest-tracking", "--player-id", "2544"])

    assert result.exit_code == 1


def test_advanced_stats_tracking_db_error():
    with patch(
        "nba_vault.cli.advanced_stats.get_db_connection", side_effect=RuntimeError("locked")
    ):
        result = runner.invoke(app, ["advanced-stats", "ingest-tracking", "--player-id", "2544"])

    assert result.exit_code == 1


def test_advanced_stats_lineups_league(patch_db_connection, patch_create_ingestor):
    result = runner.invoke(app, ["advanced-stats", "ingest-lineups", "--scope", "league"])

    assert result.exit_code == 0


def test_advanced_stats_lineups_team_id(patch_db_connection, patch_create_ingestor):
    result = runner.invoke(app, ["advanced-stats", "ingest-lineups", "--team-id", "1610612747"])

    assert result.exit_code == 0


def test_advanced_stats_lineups_scope_team_prefix(patch_db_connection, patch_create_ingestor):
    result = runner.invoke(app, ["advanced-stats", "ingest-lineups", "--scope", "team:1610612747"])

    assert result.exit_code == 0


def test_advanced_stats_lineups_failure(patch_db_connection, mock_ingestor, patch_create_ingestor):
    patch_create_ingestor.return_value = mock_ingestor(status="FAILED", error_message="error")

    result = runner.invoke(app, ["advanced-stats", "ingest-lineups"])

    assert result.exit_code == 1


def test_advanced_stats_team_other_stats_by_game(patch_db_connection, patch_create_ingestor):
    result = runner.invoke(
        app, ["advanced-stats", "ingest-team-other-stats", "--game-id", "0022300001"]
    )

    assert result.exit_code == 0


def test_advanced_stats_team_other_stats_by_team(patch_db_connection, patch_create_ingestor):
    result = runner.invoke(
        app, ["advanced-stats", "ingest-team-other-stats", "--team-id", "1610612747"]
    )

    assert result.exit_code == 0


def test_advanced_stats_team_other_stats_no_id(patch_db_connection, patch_create_ingestor):
    result = runner.invoke(app, ["advanced-stats", "ingest-team-other-stats"])

    assert result.exit_code == 1
    assert "Must specify" in result.output


def test_advanced_stats_team_other_stats_failure(
    patch_db_connection, mock_ingestor, patch_create_ingestor
):
    patch_create_ingestor.return_value = mock_ingestor(status="FAILED", error_message="error")

    result = runner.invoke(
        app, ["advanced-stats", "ingest-team-other-stats", "--game-id", "0022300001"]
    )

    assert result.exit_code == 1


def test_advanced_stats_team_advanced_league(patch_db_connection, patch_create_ingestor):
    result = runner.invoke(
        app, ["advanced-stats", "ingest-team-advanced-stats", "--scope", "league"]
    )

    assert result.exit_code == 0


def test_advanced_stats_team_advanced_by_team_id(patch_db_connection, patch_create_ingestor):
    result = runner.invoke(
        app, ["advanced-stats", "ingest-team-advanced-stats", "--team-id", "1610612747"]
    )

    assert result.exit_code == 0


def test_advanced_stats_team_advanced_failure(
    patch_db_connection, mock_ingestor, patch_create_ingestor
):
    patch_create_ingestor.return_value = mock_ingestor(status="FAILED", error_message="error")

    result = runner.invoke(app, ["advanced-stats", "ingest-team-advanced-stats"])

    assert result.exit_code == 1


def test_advanced_stats_team_advanced_db_error():
    with patch(
        "nba_vault.cli.advanced_stats.get_db_connection", side_effect=RuntimeError("locked")
    ):
        result = runner.invoke(app, ["advanced-stats", "ingest-team-advanced-stats"])

    assert result.exit_code == 1
