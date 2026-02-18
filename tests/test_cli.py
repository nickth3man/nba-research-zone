"""Tests for CLI commands."""

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

runner = CliRunner()


def _mock_conn():
    """Return a mock SQLite connection with sensible cursor defaults."""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = (0,)
    conn.execute.return_value = cursor
    return conn


def _mock_ingestor(status="SUCCESS", rows=5):
    ingestor = MagicMock()
    ingestor.ingest.return_value = {"status": status, "rows_affected": rows}
    return ingestor


# ---------------------------------------------------------------------------
# admin init
# ---------------------------------------------------------------------------


def test_admin_init_success():
    from nba_vault.cli import app

    with patch("nba_vault.cli.admin.init_database") as mock_init:
        result = runner.invoke(app, ["admin", "init"])

    assert result.exit_code == 0
    mock_init.assert_called_once()


def test_admin_init_failure():
    from nba_vault.cli import app

    with patch("nba_vault.cli.admin.init_database", side_effect=RuntimeError("disk full")):
        result = runner.invoke(app, ["admin", "init"])

    assert result.exit_code == 1
    assert "FAIL" in result.output


# ---------------------------------------------------------------------------
# admin migrate
# ---------------------------------------------------------------------------


def test_admin_migrate_apply():
    from nba_vault.cli import app

    with patch("nba_vault.schema.migrations.run_migrations") as mock_run:
        result = runner.invoke(app, ["admin", "migrate"])

    assert result.exit_code == 0
    mock_run.assert_called_once()


def test_admin_migrate_rollback():
    from nba_vault.cli import app

    with patch("nba_vault.cli.admin.rollback_migration") as mock_rb:
        result = runner.invoke(app, ["admin", "migrate", "--rollback"])

    assert result.exit_code == 0
    mock_rb.assert_called_once()


def test_admin_migrate_rollback_steps():
    from nba_vault.cli import app

    with patch("nba_vault.cli.admin.rollback_migration") as mock_rb:
        result = runner.invoke(app, ["admin", "migrate", "--rollback", "--steps", "3"])

    assert result.exit_code == 0
    mock_rb.assert_called_once_with(steps=3)


def test_admin_migrate_failure():
    from nba_vault.cli import app

    with patch("nba_vault.cli.admin.rollback_migration", side_effect=RuntimeError("failed")):
        result = runner.invoke(app, ["admin", "migrate", "--rollback"])

    assert result.exit_code == 1
    assert "FAIL" in result.output


# ---------------------------------------------------------------------------
# admin status
# ---------------------------------------------------------------------------


def test_admin_status_no_database(tmp_path):
    from nba_vault.cli import app

    mock_settings = MagicMock()
    mock_settings.db_path = str(tmp_path / "nonexistent.db")

    with patch("nba_vault.cli.admin.get_settings", return_value=mock_settings):
        result = runner.invoke(app, ["admin", "status"])

    assert result.exit_code == 1
    assert "not found" in result.output


def test_admin_status_success(tmp_path):
    from nba_vault.cli import app

    real_db = tmp_path / "test.db"
    real_db.touch()

    mock_settings = MagicMock()
    mock_settings.db_path = str(real_db)

    with (
        patch("nba_vault.cli.admin.get_settings", return_value=mock_settings),
        patch("nba_vault.cli.admin.get_db_connection", return_value=_mock_conn()),
    ):
        result = runner.invoke(app, ["admin", "status"])

    assert result.exit_code == 0
    assert "Database Status" in result.output


def test_admin_status_db_open_error(tmp_path):
    from nba_vault.cli import app

    real_db = tmp_path / "test.db"
    real_db.touch()

    mock_settings = MagicMock()
    mock_settings.db_path = str(real_db)

    with (
        patch("nba_vault.cli.admin.get_settings", return_value=mock_settings),
        patch("nba_vault.cli.admin.get_db_connection", side_effect=RuntimeError("locked")),
    ):
        result = runner.invoke(app, ["admin", "status"])

    assert result.exit_code == 1


def test_admin_status_query_error(tmp_path):
    from nba_vault.cli import app

    real_db = tmp_path / "test.db"
    real_db.touch()

    mock_settings = MagicMock()
    mock_settings.db_path = str(real_db)

    bad_conn = MagicMock()
    bad_conn.execute.side_effect = Exception("query failed")

    with (
        patch("nba_vault.cli.admin.get_settings", return_value=mock_settings),
        patch("nba_vault.cli.admin.get_db_connection", return_value=bad_conn),
    ):
        result = runner.invoke(app, ["admin", "status"])

    assert result.exit_code == 1


# ---------------------------------------------------------------------------
# admin validate
# ---------------------------------------------------------------------------


def test_admin_validate_not_implemented():
    from nba_vault.cli import app

    with patch("nba_vault.cli.admin.get_db_connection", return_value=_mock_conn()):
        result = runner.invoke(app, ["admin", "validate"])

    assert result.exit_code == 1
    assert "not yet implemented" in result.output


def test_admin_validate_db_open_error():
    from nba_vault.cli import app

    with patch("nba_vault.cli.admin.get_db_connection", side_effect=RuntimeError("locked")):
        result = runner.invoke(app, ["admin", "validate"])

    assert result.exit_code == 1


# ---------------------------------------------------------------------------
# ingestion ingest
# ---------------------------------------------------------------------------


def test_ingestion_ingest_incremental():
    from nba_vault.cli import app

    result = runner.invoke(app, ["ingestion", "ingest"])

    assert result.exit_code == 1
    assert "not yet implemented" in result.output


def test_ingestion_ingest_full():
    from nba_vault.cli import app

    result = runner.invoke(app, ["ingestion", "ingest", "--mode", "full"])

    assert result.exit_code == 1


# ---------------------------------------------------------------------------
# ingestion ingest-players
# ---------------------------------------------------------------------------


def test_ingestion_players_by_season():
    from nba_vault.cli import app

    with (
        patch("nba_vault.cli.ingestion.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=_mock_ingestor()),
    ):
        result = runner.invoke(app, ["ingestion", "ingest-players", "--season-end-year", "2024"])

    assert result.exit_code == 0
    assert "Successfully ingested" in result.output


def test_ingestion_players_default_season():
    from nba_vault.cli import app

    with (
        patch("nba_vault.cli.ingestion.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=_mock_ingestor()),
    ):
        result = runner.invoke(app, ["ingestion", "ingest-players"])

    assert result.exit_code == 0


def test_ingestion_players_by_id():
    from nba_vault.cli import app

    with (
        patch("nba_vault.cli.ingestion.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=_mock_ingestor()),
    ):
        result = runner.invoke(app, ["ingestion", "ingest-players", "--player-id", "jamesle01"])

    assert result.exit_code == 0
    assert "jamesle01" in result.output


def test_ingestion_players_db_error():
    from nba_vault.cli import app

    with patch("nba_vault.cli.ingestion.get_db_connection", side_effect=RuntimeError("locked")):
        result = runner.invoke(app, ["ingestion", "ingest-players"])

    assert result.exit_code == 1


def test_ingestion_players_ingest_failure():
    from nba_vault.cli import app

    failed = MagicMock()
    failed.ingest.return_value = {"status": "FAILED", "error_message": "API error"}

    with (
        patch("nba_vault.cli.ingestion.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=failed),
    ):
        result = runner.invoke(app, ["ingestion", "ingest-players"])

    assert result.exit_code == 1
    assert "API error" in result.output


# ---------------------------------------------------------------------------
# advanced-stats ingest-tracking
# ---------------------------------------------------------------------------


def test_advanced_stats_tracking_by_player():
    from nba_vault.cli import app

    with (
        patch("nba_vault.cli.advanced_stats.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=_mock_ingestor()),
    ):
        result = runner.invoke(app, ["advanced-stats", "ingest-tracking", "--player-id", "2544"])

    assert result.exit_code == 0
    assert "Successfully ingested" in result.output


def test_advanced_stats_tracking_by_team():
    from nba_vault.cli import app

    with (
        patch("nba_vault.cli.advanced_stats.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=_mock_ingestor()),
    ):
        result = runner.invoke(
            app, ["advanced-stats", "ingest-tracking", "--team-id", "1610612747"]
        )

    assert result.exit_code == 0


def test_advanced_stats_tracking_no_id():
    from nba_vault.cli import app

    with (
        patch("nba_vault.cli.advanced_stats.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=_mock_ingestor()),
    ):
        result = runner.invoke(app, ["advanced-stats", "ingest-tracking"])

    assert result.exit_code == 1
    assert "Must specify" in result.output


def test_advanced_stats_tracking_failure():
    from nba_vault.cli import app

    failed = MagicMock()
    failed.ingest.return_value = {"status": "FAILED", "error_message": "API error"}

    with (
        patch("nba_vault.cli.advanced_stats.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=failed),
    ):
        result = runner.invoke(app, ["advanced-stats", "ingest-tracking", "--player-id", "2544"])

    assert result.exit_code == 1


def test_advanced_stats_tracking_db_error():
    from nba_vault.cli import app

    with patch(
        "nba_vault.cli.advanced_stats.get_db_connection", side_effect=RuntimeError("locked")
    ):
        result = runner.invoke(app, ["advanced-stats", "ingest-tracking", "--player-id", "2544"])

    assert result.exit_code == 1


# ---------------------------------------------------------------------------
# advanced-stats ingest-lineups
# ---------------------------------------------------------------------------


def test_advanced_stats_lineups_league():
    from nba_vault.cli import app

    with (
        patch("nba_vault.cli.advanced_stats.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=_mock_ingestor()),
    ):
        result = runner.invoke(app, ["advanced-stats", "ingest-lineups", "--scope", "league"])

    assert result.exit_code == 0


def test_advanced_stats_lineups_team_id():
    from nba_vault.cli import app

    with (
        patch("nba_vault.cli.advanced_stats.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=_mock_ingestor()),
    ):
        result = runner.invoke(app, ["advanced-stats", "ingest-lineups", "--team-id", "1610612747"])

    assert result.exit_code == 0


def test_advanced_stats_lineups_scope_team_prefix():
    from nba_vault.cli import app

    with (
        patch("nba_vault.cli.advanced_stats.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=_mock_ingestor()),
    ):
        result = runner.invoke(
            app, ["advanced-stats", "ingest-lineups", "--scope", "team:1610612747"]
        )

    assert result.exit_code == 0


def test_advanced_stats_lineups_failure():
    from nba_vault.cli import app

    failed = MagicMock()
    failed.ingest.return_value = {"status": "FAILED", "error_message": "error"}

    with (
        patch("nba_vault.cli.advanced_stats.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=failed),
    ):
        result = runner.invoke(app, ["advanced-stats", "ingest-lineups"])

    assert result.exit_code == 1


# ---------------------------------------------------------------------------
# advanced-stats ingest-team-other-stats
# ---------------------------------------------------------------------------


def test_advanced_stats_team_other_stats_by_game():
    from nba_vault.cli import app

    with (
        patch("nba_vault.cli.advanced_stats.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=_mock_ingestor()),
    ):
        result = runner.invoke(
            app,
            ["advanced-stats", "ingest-team-other-stats", "--game-id", "0022300001"],
        )

    assert result.exit_code == 0


def test_advanced_stats_team_other_stats_by_team():
    from nba_vault.cli import app

    with (
        patch("nba_vault.cli.advanced_stats.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=_mock_ingestor()),
    ):
        result = runner.invoke(
            app,
            ["advanced-stats", "ingest-team-other-stats", "--team-id", "1610612747"],
        )

    assert result.exit_code == 0


def test_advanced_stats_team_other_stats_no_id():
    from nba_vault.cli import app

    with (
        patch("nba_vault.cli.advanced_stats.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=_mock_ingestor()),
    ):
        result = runner.invoke(app, ["advanced-stats", "ingest-team-other-stats"])

    assert result.exit_code == 1
    assert "Must specify" in result.output


def test_advanced_stats_team_other_stats_failure():
    from nba_vault.cli import app

    failed = MagicMock()
    failed.ingest.return_value = {"status": "FAILED", "error_message": "error"}

    with (
        patch("nba_vault.cli.advanced_stats.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=failed),
    ):
        result = runner.invoke(
            app,
            ["advanced-stats", "ingest-team-other-stats", "--game-id", "0022300001"],
        )

    assert result.exit_code == 1


# ---------------------------------------------------------------------------
# advanced-stats ingest-team-advanced-stats
# ---------------------------------------------------------------------------


def test_advanced_stats_team_advanced_league():
    from nba_vault.cli import app

    with (
        patch("nba_vault.cli.advanced_stats.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=_mock_ingestor()),
    ):
        result = runner.invoke(
            app, ["advanced-stats", "ingest-team-advanced-stats", "--scope", "league"]
        )

    assert result.exit_code == 0


def test_advanced_stats_team_advanced_by_team_id():
    from nba_vault.cli import app

    with (
        patch("nba_vault.cli.advanced_stats.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=_mock_ingestor()),
    ):
        result = runner.invoke(
            app,
            ["advanced-stats", "ingest-team-advanced-stats", "--team-id", "1610612747"],
        )

    assert result.exit_code == 0


def test_advanced_stats_team_advanced_failure():
    from nba_vault.cli import app

    failed = MagicMock()
    failed.ingest.return_value = {"status": "FAILED", "error_message": "error"}

    with (
        patch("nba_vault.cli.advanced_stats.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=failed),
    ):
        result = runner.invoke(app, ["advanced-stats", "ingest-team-advanced-stats"])

    assert result.exit_code == 1


def test_advanced_stats_team_advanced_db_error():
    from nba_vault.cli import app

    with patch(
        "nba_vault.cli.advanced_stats.get_db_connection", side_effect=RuntimeError("locked")
    ):
        result = runner.invoke(app, ["advanced-stats", "ingest-team-advanced-stats"])

    assert result.exit_code == 1


# ---------------------------------------------------------------------------
# scrapers ingest-injuries
# ---------------------------------------------------------------------------


def test_scrapers_injuries_all():
    from nba_vault.cli import app

    with (
        patch("nba_vault.cli.scrapers.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=_mock_ingestor()),
    ):
        result = runner.invoke(app, ["scrapers", "ingest-injuries"])

    assert result.exit_code == 0
    assert "Successfully ingested" in result.output


def test_scrapers_injuries_by_team():
    from nba_vault.cli import app

    with (
        patch("nba_vault.cli.scrapers.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=_mock_ingestor()),
    ):
        result = runner.invoke(app, ["scrapers", "ingest-injuries", "--team", "LAL"])

    assert result.exit_code == 0
    assert "LAL" in result.output


def test_scrapers_injuries_failure():
    from nba_vault.cli import app

    failed = MagicMock()
    failed.ingest.return_value = {"status": "FAILED", "error_message": "scrape error"}

    with (
        patch("nba_vault.cli.scrapers.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=failed),
    ):
        result = runner.invoke(app, ["scrapers", "ingest-injuries"])

    assert result.exit_code == 1
    assert "scrape error" in result.output


def test_scrapers_injuries_db_error():
    from nba_vault.cli import app

    with patch("nba_vault.cli.scrapers.get_db_connection", side_effect=RuntimeError("locked")):
        result = runner.invoke(app, ["scrapers", "ingest-injuries"])

    assert result.exit_code == 1


# ---------------------------------------------------------------------------
# scrapers ingest-contracts
# ---------------------------------------------------------------------------


def test_scrapers_contracts_all():
    from nba_vault.cli import app

    with (
        patch("nba_vault.cli.scrapers.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=_mock_ingestor()),
    ):
        result = runner.invoke(app, ["scrapers", "ingest-contracts"])

    assert result.exit_code == 0
    assert "Successfully ingested" in result.output


def test_scrapers_contracts_by_team():
    from nba_vault.cli import app

    with (
        patch("nba_vault.cli.scrapers.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=_mock_ingestor()),
    ):
        result = runner.invoke(app, ["scrapers", "ingest-contracts", "--team", "LAL"])

    assert result.exit_code == 0
    assert "LAL" in result.output


def test_scrapers_contracts_failure():
    from nba_vault.cli import app

    failed = MagicMock()
    failed.ingest.return_value = {"status": "FAILED", "error_message": "contract error"}

    with (
        patch("nba_vault.cli.scrapers.get_db_connection", return_value=_mock_conn()),
        patch("nba_vault.ingestion.create_ingestor", return_value=failed),
    ):
        result = runner.invoke(app, ["scrapers", "ingest-contracts"])

    assert result.exit_code == 1


def test_scrapers_contracts_db_error():
    from nba_vault.cli import app

    with patch("nba_vault.cli.scrapers.get_db_connection", side_effect=RuntimeError("locked")):
        result = runner.invoke(app, ["scrapers", "ingest-contracts"])

    assert result.exit_code == 1


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------


def test_export_duckdb_success():
    from nba_vault.cli import app

    with patch("nba_vault.cli.export.build_duckdb_database") as mock_build:
        result = runner.invoke(app, ["export", "export", "--format", "duckdb"])

    assert result.exit_code == 0
    mock_build.assert_called_once()
    assert "DuckDB database built" in result.output


def test_export_unsupported_format():
    from nba_vault.cli import app

    result = runner.invoke(app, ["export", "export", "--format", "parquet"])

    assert result.exit_code == 1
    assert "not yet implemented" in result.output


def test_export_duckdb_failure():
    from nba_vault.cli import app

    with patch(
        "nba_vault.cli.export.build_duckdb_database",
        side_effect=RuntimeError("build failed"),
    ):
        result = runner.invoke(app, ["export", "export", "--format", "duckdb"])

    assert result.exit_code == 1
    assert "FAIL" in result.output
