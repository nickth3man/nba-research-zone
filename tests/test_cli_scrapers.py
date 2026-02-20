"""Tests for scrapers CLI commands."""

from unittest.mock import patch

from typer.testing import CliRunner

from nba_vault.cli import app

runner = CliRunner()


def test_scrapers_injuries_all(patch_db_connection, patch_create_ingestor):
    result = runner.invoke(app, ["scrapers", "ingest-injuries"])

    assert result.exit_code == 0
    assert "Successfully ingested" in result.output


def test_scrapers_injuries_by_team(patch_db_connection, patch_create_ingestor):
    result = runner.invoke(app, ["scrapers", "ingest-injuries", "--team", "LAL"])

    assert result.exit_code == 0
    assert "LAL" in result.output


def test_scrapers_injuries_failure(patch_db_connection, mock_ingestor, patch_create_ingestor):
    patch_create_ingestor.return_value = mock_ingestor(
        status="FAILED", error_message="scrape error"
    )

    result = runner.invoke(app, ["scrapers", "ingest-injuries"])

    assert result.exit_code == 1
    assert "scrape error" in result.output


def test_scrapers_injuries_db_error():
    with patch("nba_vault.cli.scrapers.get_db_connection", side_effect=RuntimeError("locked")):
        result = runner.invoke(app, ["scrapers", "ingest-injuries"])

    assert result.exit_code == 1


def test_scrapers_contracts_all(patch_db_connection, patch_create_ingestor):
    result = runner.invoke(app, ["scrapers", "ingest-contracts"])

    assert result.exit_code == 0
    assert "Successfully ingested" in result.output


def test_scrapers_contracts_by_team(patch_db_connection, patch_create_ingestor):
    result = runner.invoke(app, ["scrapers", "ingest-contracts", "--team", "LAL"])

    assert result.exit_code == 0
    assert "LAL" in result.output


def test_scrapers_contracts_failure(patch_db_connection, mock_ingestor, patch_create_ingestor):
    patch_create_ingestor.return_value = mock_ingestor(
        status="FAILED", error_message="contract error"
    )

    result = runner.invoke(app, ["scrapers", "ingest-contracts"])

    assert result.exit_code == 1


def test_scrapers_contracts_db_error():
    with patch("nba_vault.cli.scrapers.get_db_connection", side_effect=RuntimeError("locked")):
        result = runner.invoke(app, ["scrapers", "ingest-contracts"])

    assert result.exit_code == 1
