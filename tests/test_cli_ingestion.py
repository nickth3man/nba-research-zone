"""Tests for ingestion CLI commands."""

from unittest.mock import patch

from typer.testing import CliRunner

from nba_vault.cli import app

runner = CliRunner()


def test_ingestion_ingest_incremental():
    with patch("nba_vault.cli.ingestion.get_db_connection", side_effect=RuntimeError("no db")):
        result = runner.invoke(app, ["ingestion", "ingest"])

    assert result.exit_code == 1


def test_ingestion_ingest_full():
    with patch("nba_vault.cli.ingestion.get_db_connection", side_effect=RuntimeError("no db")):
        result = runner.invoke(app, ["ingestion", "ingest", "--mode", "full"])

    assert result.exit_code == 1


def test_ingestion_players_by_season(patch_db_connection, patch_create_ingestor):
    result = runner.invoke(app, ["ingestion", "ingest-players", "--season-end-year", "2024"])

    assert result.exit_code == 0
    assert "Successfully ingested" in result.output


def test_ingestion_players_default_season(patch_db_connection, patch_create_ingestor):
    result = runner.invoke(app, ["ingestion", "ingest-players"])

    assert result.exit_code == 0


def test_ingestion_players_by_id(patch_db_connection, patch_create_ingestor):
    result = runner.invoke(app, ["ingestion", "ingest-players", "--player-id", "jamesle01"])

    assert result.exit_code == 0
    assert "jamesle01" in result.output


def test_ingestion_players_db_error():
    with patch("nba_vault.cli.ingestion.get_db_connection", side_effect=RuntimeError("locked")):
        result = runner.invoke(app, ["ingestion", "ingest-players"])

    assert result.exit_code == 1


def test_ingestion_players_ingest_failure(
    patch_db_connection, mock_ingestor, patch_create_ingestor
):
    patch_create_ingestor.return_value = mock_ingestor(status="FAILED", error_message="API error")

    result = runner.invoke(app, ["ingestion", "ingest-players"])

    assert result.exit_code == 1
    assert "API error" in result.output
