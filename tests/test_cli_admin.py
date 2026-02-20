"""Tests for admin CLI commands."""

from unittest.mock import patch

from typer.testing import CliRunner

from nba_vault.cli import app

runner = CliRunner()


def test_admin_init_success():
    with patch("nba_vault.cli.admin.init_database") as mock_init:
        result = runner.invoke(app, ["admin", "init"])

    assert result.exit_code == 0
    mock_init.assert_called_once()


def test_admin_init_failure():
    with patch("nba_vault.cli.admin.init_database", side_effect=RuntimeError("disk full")):
        result = runner.invoke(app, ["admin", "init"])

    assert result.exit_code == 1
    assert "FAIL" in result.output


def test_admin_migrate_apply():
    with patch("nba_vault.schema.migrations.run_migrations") as mock_run:
        result = runner.invoke(app, ["admin", "migrate"])

    assert result.exit_code == 0
    mock_run.assert_called_once()


def test_admin_migrate_rollback():
    with patch("nba_vault.cli.admin.rollback_migration") as mock_rb:
        result = runner.invoke(app, ["admin", "migrate", "--rollback"])

    assert result.exit_code == 0
    mock_rb.assert_called_once()


def test_admin_migrate_rollback_steps():
    with patch("nba_vault.cli.admin.rollback_migration") as mock_rb:
        result = runner.invoke(app, ["admin", "migrate", "--rollback", "--steps", "3"])

    assert result.exit_code == 0
    mock_rb.assert_called_once_with(steps=3)


def test_admin_migrate_failure():
    with patch("nba_vault.cli.admin.rollback_migration", side_effect=RuntimeError("failed")):
        result = runner.invoke(app, ["admin", "migrate", "--rollback"])

    assert result.exit_code == 1
    assert "FAIL" in result.output


def test_admin_status_no_database(tmp_path, patch_settings):
    patch_settings.db_path = str(tmp_path / "nonexistent.db")
    result = runner.invoke(app, ["admin", "status"])

    assert result.exit_code == 1
    assert "not found" in result.output


def test_admin_status_success(tmp_path, patch_settings, patch_db_connection):
    real_db = tmp_path / "test.db"
    real_db.touch()
    patch_settings.db_path = str(real_db)

    result = runner.invoke(app, ["admin", "status"])

    assert result.exit_code == 0
    assert "Database Status" in result.output


def test_admin_status_db_open_error(tmp_path, patch_settings):
    real_db = tmp_path / "test.db"
    real_db.touch()
    patch_settings.db_path = str(real_db)

    with patch("nba_vault.cli.admin.get_db_connection", side_effect=RuntimeError("locked")):
        result = runner.invoke(app, ["admin", "status"])

    assert result.exit_code == 1


def test_admin_status_query_error(tmp_path, patch_settings, patch_db_connection):
    real_db = tmp_path / "test.db"
    real_db.touch()
    patch_settings.db_path = str(real_db)

    patch_db_connection.execute.side_effect = Exception("query failed")

    result = runner.invoke(app, ["admin", "status"])

    assert result.exit_code == 1


def test_admin_validate_runs_checks(patch_db_connection):
    result = runner.invoke(app, ["admin", "validate"])

    assert result.exit_code == 1
    assert "validation" in result.output.lower() or "check" in result.output.lower()


def test_admin_validate_db_open_error():
    with patch("nba_vault.cli.admin.get_db_connection", side_effect=RuntimeError("locked")):
        result = runner.invoke(app, ["admin", "validate"])

    assert result.exit_code == 1
