"""Tests for export CLI commands."""

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from nba_vault.cli import app

runner = CliRunner()


def test_export_duckdb_success():
    with patch("nba_vault.cli.export.build_duckdb_database") as mock_build:
        result = runner.invoke(app, ["export", "export", "--format", "duckdb"])

    assert result.exit_code == 0
    mock_build.assert_called_once()
    assert "DuckDB database built" in result.output


def test_export_unsupported_format():
    result = runner.invoke(app, ["export", "export", "--format", "xml"])

    assert result.exit_code == 1
    assert "Unknown export format" in result.output


def test_export_duckdb_failure():
    with patch(
        "nba_vault.cli.export.build_duckdb_database",
        side_effect=RuntimeError("build failed"),
    ):
        result = runner.invoke(app, ["export", "export", "--format", "duckdb"])

    assert result.exit_code == 1
    assert "FAIL" in result.output


def test_export_csv_empty_db(patch_db_connection):
    # Simulate empty database - no user tables
    patch_db_connection.cursor.return_value.fetchall.return_value = [
        ("_yoyo_log",),
        ("_yoyo_migration",),
        ("sqlite_sequence",),
    ]

    result = runner.invoke(app, ["export", "export", "--format", "csv"])

    assert result.exit_code == 0
    assert "Exporting to CSV format" in result.output


def test_export_csv_success(patch_db_connection):
    mock_cursor = MagicMock()
    mock_cursor.execute.return_value = None
    mock_cursor.fetchone.side_effect = None
    mock_cursor.fetchall.side_effect = [
        [("league",), ("season",), ("player",), ("_yoyo_log",)],
        [
            ("NBA", "National Basketball Association", 1946, None),
            ("ABA", "American Basketball Association", 1967, 1976),
        ],
        [(2024, "NBA", "2024-25", 82, "2024-10-22", "2025-04-15", None, None)],
        [(2544, "LeBron", "James", "LeBron James", "1984-12-30", None, None, "USA")],
    ]
    mock_cursor.description = [
        MagicMock(__getitem__=lambda self, x: "league_id"),
        MagicMock(__getitem__=lambda self, x: "league_name"),
        MagicMock(__getitem__=lambda self, x: "founded_year"),
        MagicMock(__getitem__=lambda self, x: "folded_year"),
    ]
    patch_db_connection.cursor.return_value = mock_cursor

    result = runner.invoke(app, ["export", "export", "--format", "csv", "-o", "test_out"])

    assert result.exit_code == 0
    assert "Exporting to CSV format" in result.output
    assert "league: 2 rows" in result.output


def test_export_json_success(patch_db_connection):
    mock_cursor = MagicMock()
    mock_cursor.execute.return_value = None
    mock_cursor.fetchall.side_effect = [
        [("league",), ("_yoyo_log",)],
        [("NBA", "National Basketball Association", 1946, None)],
    ]
    mock_cursor.description = [
        MagicMock(__getitem__=lambda self, x: "league_id"),
        MagicMock(__getitem__=lambda self, x: "league_name"),
        MagicMock(__getitem__=lambda self, x: "founded_year"),
        MagicMock(__getitem__=lambda self, x: "folded_year"),
    ]
    patch_db_connection.cursor.return_value = mock_cursor

    result = runner.invoke(app, ["export", "export", "--format", "json"])

    assert result.exit_code == 0
    assert "Exporting to JSON format" in result.output
    assert "league: 1 rows" in result.output


def test_export_parquet_success(patch_db_connection):
    from pathlib import Path

    with (
        patch("nba_vault.cli.export.pa.table") as mock_pa_table,
        patch("nba_vault.cli.export.pq.write_table") as mock_write,
    ):
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        mock_cursor.fetchall.side_effect = [
            [("league",), ("_yoyo_log",)],
            [("NBA", "National Basketball Association", 1946, None)],
        ]
        mock_cursor.description = [
            MagicMock(__getitem__=lambda self, x: "league_id"),
            MagicMock(__getitem__=lambda self, x: "league_name"),
            MagicMock(__getitem__=lambda self, x: "founded_year"),
            MagicMock(__getitem__=lambda self, x: "folded_year"),
        ]
        patch_db_connection.cursor.return_value = mock_cursor

        # Mock PyArrow table creation
        mock_arrow_table = MagicMock()
        mock_arrow_table.num_rows = 1
        mock_pa_table.return_value = mock_arrow_table

        # Mock write_table to create an actual file for size checking
        def side_effect_write(table, path):
            path_obj = Path(path)
            path_obj.parent.mkdir(parents=True, exist_ok=True)
            path_obj.write_bytes(b"mock parquet data")

        mock_write.side_effect = side_effect_write

        result = runner.invoke(app, ["export", "export", "--format", "parquet", "-o", "test_out"])

        # Cleanup
        import shutil

        test_out = Path("test_out")
        if test_out.exists():
            shutil.rmtree(test_out)

    assert result.exit_code == 0
    assert "Exporting to Parquet format" in result.output
    assert "league: 1 rows" in result.output


def test_export_with_entity_filter(patch_db_connection):
    mock_cursor = MagicMock()
    mock_cursor.execute.return_value = None
    mock_cursor.fetchall.side_effect = [
        [("league",), ("season",), ("player",)],
        [("NBA", "National Basketball Association", 1946, None)],
    ]
    mock_cursor.description = [
        MagicMock(__getitem__=lambda self, x: "league_id"),
        MagicMock(__getitem__=lambda self, x: "league_name"),
        MagicMock(__getitem__=lambda self, x: "founded_year"),
        MagicMock(__getitem__=lambda self, x: "folded_year"),
    ]
    patch_db_connection.cursor.return_value = mock_cursor

    result = runner.invoke(app, ["export", "export", "--format", "csv", "--entity", "league"])

    assert result.exit_code == 0
    assert "league: 1 rows" in result.output


def test_export_invalid_table_name(patch_db_connection):
    mock_cursor = MagicMock()
    mock_cursor.execute.return_value = None
    mock_cursor.fetchall.side_effect = [
        [("league;DROP TABLE",), ("valid_table",)],
    ]
    patch_db_connection.cursor.return_value = mock_cursor

    result = runner.invoke(app, ["export", "export", "--format", "csv"])

    assert result.exit_code == 1
    assert "Invalid table name" in result.output
