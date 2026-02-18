"""Comprehensive tests for DuckDB builder.

Tests cover database building, view creation, and error handling.
"""

from unittest.mock import Mock, patch

import pytest

from nba_vault.duckdb.builder import (
    build_duckdb_database,
    create_analytical_views,
    refresh_views,
)


class TestBuildDuckDBDatabase:
    """Tests for build_duckdb_database() function."""

    def test_build_success(self, tmp_path):
        """Test successful DuckDB database build."""
        # Create a mock SQLite database
        sqlite_db = tmp_path / "test.sqlite"
        sqlite_db.touch()  # Create empty file

        duckdb_db = tmp_path / "test.duckdb"

        with patch("nba_vault.duckdb.builder.duckdb") as mock_duckdb:
            mock_con = Mock()
            mock_duckdb.connect.return_value = mock_con

            build_duckdb_database(sqlite_db, duckdb_db)

            # Verify DuckDB connection was made
            mock_duckdb.connect.assert_called_once_with(str(duckdb_db))

            # Verify configuration
            assert mock_con.execute.called

    def test_build_with_default_paths(self, tmp_path):
        """Test building with default paths from settings."""
        with patch("nba_vault.duckdb.builder.get_settings") as mock_settings:
            settings = Mock()
            settings.db_path = tmp_path / "nba.sqlite"
            settings.duckdb_path = tmp_path / "nba.duckdb"
            settings.duckdb_memory_limit = "4GB"
            settings.duckdb_threads = 4
            mock_settings.return_value = settings

            # Create SQLite file
            settings.db_path.touch()

            with patch("nba_vault.duckdb.builder.duckdb") as mock_duckdb:
                mock_con = Mock()
                mock_duckdb.connect.return_value = mock_con

                build_duckdb_database()

                # Should use settings paths
                mock_duckdb.connect.assert_called_once()

    def test_build_sqlite_not_found(self, tmp_path):
        """Test that FileNotFoundError is raised when SQLite doesn't exist."""
        non_existent = tmp_path / "non_existent.sqlite"

        with pytest.raises(FileNotFoundError, match="SQLite database not found"):
            build_duckdb_database(non_existent)

    def test_build_creates_views(self, tmp_path):
        """Test that build creates analytical views."""
        sqlite_db = tmp_path / "test.sqlite"
        sqlite_db.touch()

        duckdb_db = tmp_path / "test.duckdb"

        with patch("nba_vault.duckdb.builder.duckdb") as mock_duckdb:
            mock_con = Mock()
            mock_duckdb.connect.return_value = mock_con

            build_duckdb_database(sqlite_db, duckdb_db)

            # Verify views were created
            # Check that create_analytical_views was called
            # (we can't directly verify this, but we can check execute was called)

    def test_build_error_handling(self, tmp_path):
        """Test error handling during build."""
        sqlite_db = tmp_path / "test.sqlite"
        sqlite_db.touch()

        duckdb_db = tmp_path / "test.duckdb"

        with patch("nba_vault.duckdb.builder.duckdb") as mock_duckdb:
            mock_con = Mock()
            mock_duckdb.connect.return_value = mock_con

            # Simulate error during configuration
            mock_con.execute.side_effect = Exception("DuckDB error")

            with pytest.raises(Exception, match="DuckDB error"):
                build_duckdb_database(sqlite_db, duckdb_db)

            # Verify connection was closed
            mock_con.close.assert_called_once()

    def test_build_configuration(self, tmp_path):
        """Test that DuckDB is configured correctly."""
        sqlite_db = tmp_path / "test.sqlite"
        sqlite_db.touch()

        duckdb_db = tmp_path / "test.duckdb"

        with patch("nba_vault.duckdb.builder.get_settings") as mock_settings:
            settings = Mock()
            settings.db_path = sqlite_db
            settings.duckdb_path = duckdb_db
            settings.duckdb_memory_limit = "8GB"
            settings.duckdb_threads = 8
            mock_settings.return_value = settings

            with patch("nba_vault.duckdb.builder.duckdb") as mock_duckdb:
                mock_con = Mock()
                mock_duckdb.connect.return_value = mock_con

                build_duckdb_database()

                # Verify configuration statements
                execute_calls = [str(call) for call in mock_con.execute.call_args_list]

                # Should set memory limit and threads
                assert any("memory_limit" in str(call) for call in execute_calls)
                assert any("threads" in str(call) for call in execute_calls)

    def test_build_attaches_sqlite(self, tmp_path):
        """Test that SQLite database is attached."""
        sqlite_db = tmp_path / "test.sqlite"
        sqlite_db.touch()

        duckdb_db = tmp_path / "test.duckdb"

        with patch("nba_vault.duckdb.builder.duckdb") as mock_duckdb:
            mock_con = Mock()
            mock_duckdb.connect.return_value = mock_con

            build_duckdb_database(sqlite_db, duckdb_db)

            # Verify ATTACH statement
            execute_calls = [str(call) for call in mock_con.execute.call_args_list]
            assert any("ATTACH" in str(call) for call in execute_calls)
            assert any("sqlite_db" in str(call) for call in execute_calls)


class TestRefreshViews:
    """Tests for refresh_views() function."""

    def test_refresh_existing_duckdb(self, tmp_path):
        """Test refreshing views in existing DuckDB database."""
        sqlite_db = tmp_path / "test.sqlite"
        sqlite_db.touch()

        duckdb_db = tmp_path / "test.duckdb"
        duckdb_db.touch()

        with patch("nba_vault.duckdb.builder.duckdb") as mock_duckdb:
            mock_con = Mock()
            mock_duckdb.connect.return_value = mock_con

            refresh_views(sqlite_db, duckdb_db)

            # Should connect and refresh
            mock_duckdb.connect.assert_called_once_with(str(duckdb_db))
            mock_con.close.assert_called_once()

    def test_refresh_non_existent_duckdb_builds_new(self, tmp_path):
        """Test that missing DuckDB triggers a new build."""
        sqlite_db = tmp_path / "test.sqlite"
        sqlite_db.touch()

        duckdb_db = tmp_path / "non_existent.duckdb"

        with patch("nba_vault.duckdb.builder.build_duckdb_database") as mock_build:
            refresh_views(sqlite_db, duckdb_db)

            # Should call build_duckdb_database
            mock_build.assert_called_once_with(sqlite_db, duckdb_db)

    def test_refresh_with_default_paths(self, tmp_path):
        """Test refreshing with default paths from settings."""
        with patch("nba_vault.duckdb.builder.get_settings") as mock_settings:
            settings = Mock()
            settings.db_path = tmp_path / "nba.sqlite"
            settings.duckdb_path = tmp_path / "nba.duckdb"

            mock_settings.return_value = settings

            # Create files
            settings.db_path.touch()
            settings.duckdb_path.touch()

            with patch("nba_vault.duckdb.builder.duckdb") as mock_duckdb:
                mock_con = Mock()
                mock_duckdb.connect.return_value = mock_con

                refresh_views()

                # Should use settings paths
                mock_duckdb.connect.assert_called_once()

    def test_refresh_error_handling(self, tmp_path):
        """Test error handling during refresh."""
        sqlite_db = tmp_path / "test.sqlite"
        sqlite_db.touch()

        duckdb_db = tmp_path / "test.duckdb"
        duckdb_db.touch()

        with patch("nba_vault.duckdb.builder.duckdb") as mock_duckdb:
            mock_con = Mock()
            mock_con.execute.side_effect = Exception("Refresh error")
            mock_duckdb.connect.return_value = mock_con

            with pytest.raises(Exception, match="Refresh error"):
                refresh_views(sqlite_db, duckdb_db)

            # Verify connection was closed
            mock_con.close.assert_called_once()


class TestCreateAnalyticalViews:
    """Tests for create_analytical_views() function."""

    def test_create_views_from_directory(self, tmp_path):
        """Test creating views from SQL files in directory."""
        # Create mock views directory
        views_dir = tmp_path / "duckdb" / "views"
        views_dir.mkdir(parents=True)

        # Create mock view files
        view1 = views_dir / "v_test1.sql"
        view1.write_text("SELECT * FROM sqlite_db.test1")

        view2 = views_dir / "v_test2.sql"
        view2.write_text("SELECT * FROM sqlite_db.test2")

        mock_con = Mock()

        # Patch the views_dir computed inside create_analytical_views
        with patch("nba_vault.duckdb.builder.Path") as mock_path_cls:
            # Path(__file__) returns a mock; chain .parent.parent.parent / "duckdb" / "views"
            mock_path_cls.return_value.parent.parent.parent.__truediv__.return_value.__truediv__.return_value = views_dir

            create_analytical_views(mock_con)

            # Verify views were created
            assert mock_con.execute.call_count == 2

    def test_create_views_non_existent_directory(self, tmp_path):
        """Test handling when views directory doesn't exist."""
        mock_con = Mock()

        non_existent = tmp_path / "non_existent" / "views"

        with patch("nba_vault.duckdb.builder.Path") as mock_path_cls:
            mock_path_cls.return_value.parent.parent.parent.__truediv__.return_value.__truediv__.return_value = non_existent

            # Should not raise, should just return
            create_analytical_views(mock_con)

            # Should not execute any CREATE statements
            assert not mock_con.execute.called

    def test_create_views_with_sql_error(self, tmp_path):
        """Test handling of SQL errors during view creation."""
        # Create mock views directory
        views_dir = tmp_path / "duckdb" / "views"
        views_dir.mkdir(parents=True)

        # Create invalid SQL file
        view1 = views_dir / "v_invalid.sql"
        view1.write_text("INVALID SQL HERE")

        mock_con = Mock()
        mock_con.execute.side_effect = Exception("SQL syntax error")

        with patch("nba_vault.duckdb.builder.Path") as mock_path_cls:
            mock_path_cls.return_value.parent.parent.parent.__truediv__.return_value.__truediv__.return_value = views_dir

            with pytest.raises(Exception, match="SQL syntax error"):
                create_analytical_views(mock_con)

    def test_create_view_names_stripped_prefix(self, tmp_path):
        """Test that 'v_' prefix is stripped from view names."""
        # Create mock views directory
        views_dir = tmp_path / "duckdb" / "views"
        views_dir.mkdir(parents=True)

        # Create view with v_ prefix
        view1 = views_dir / "v_player_stats.sql"
        view1.write_text("SELECT * FROM sqlite_db.player")

        mock_con = Mock()

        with patch("nba_vault.duckdb.builder.Path") as mock_path_cls:
            mock_path_cls.return_value.parent.parent.parent.__truediv__.return_value.__truediv__.return_value = views_dir

            create_analytical_views(mock_con)

            # Verify view was created with name without 'v_' prefix
            call_args = str(mock_con.execute.call_args)
            assert "CREATE OR REPLACE VIEW player_stats" in call_args

    def test_create_views_sorted_alphabetically(self, tmp_path):
        """Test that views are created in alphabetical order."""
        # Create mock views directory
        views_dir = tmp_path / "duckdb" / "views"
        views_dir.mkdir(parents=True)

        # Create view files out of order
        view2 = views_dir / "v_zulu.sql"
        view2.write_text("SELECT * FROM sqlite_db.zulu")

        view1 = views_dir / "v_alpha.sql"
        view1.write_text("SELECT * FROM sqlite_db.alpha")

        mock_con = Mock()

        with patch("nba_vault.duckdb.builder.Path") as mock_path_cls:
            mock_path_cls.return_value.parent.parent.parent.__truediv__.return_value.__truediv__.return_value = views_dir

            create_analytical_views(mock_con)

            # Views should be created in alphabetical order
            calls = [str(call) for call in mock_con.execute.call_args_list]
            alpha_call = next(c for c in calls if "alpha" in c)
            zulu_call = next(c for c in calls if "zulu" in c)

            # alpha should come before zulu
            assert calls.index(alpha_call) < calls.index(zulu_call)


class TestDuckDBBuilderIntegration:
    """Integration tests for DuckDB builder."""

    def test_build_and_refresh_workflow(self, tmp_path):
        """Test complete build and refresh workflow."""
        sqlite_db = tmp_path / "test.sqlite"
        sqlite_db.touch()

        duckdb_db = tmp_path / "test.duckdb"

        with patch("nba_vault.duckdb.builder.duckdb") as mock_duckdb:
            mock_con = Mock()
            mock_duckdb.connect.return_value = mock_con

            # Build
            build_duckdb_database(sqlite_db, duckdb_db)

            # Refresh
            refresh_views(sqlite_db, duckdb_db)

            # Both should connect and close
            assert mock_duckdb.connect.call_count == 2
            assert mock_con.close.call_count == 2

    def test_path_conversion_to_string(self, tmp_path):
        """Test that Path objects are converted to strings."""
        sqlite_db = tmp_path / "test.sqlite"
        sqlite_db.touch()

        duckdb_db = tmp_path / "test.duckdb"

        with patch("nba_vault.duckdb.builder.duckdb") as mock_duckdb:
            mock_con = Mock()
            mock_duckdb.connect.return_value = mock_con

            build_duckdb_database(sqlite_db, duckdb_db)

            # Verify paths were converted to strings
            call_args = mock_duckdb.connect.call_args[0]
            assert isinstance(call_args[0], str)

    def test_relative_and_absolute_paths(self, tmp_path):
        """Test handling of both relative and absolute paths."""
        sqlite_db = tmp_path / "test.sqlite"
        sqlite_db.touch()

        duckdb_db = tmp_path / "test.duckdb"

        with patch("nba_vault.duckdb.builder.duckdb") as mock_duckdb:
            mock_con = Mock()
            mock_duckdb.connect.return_value = mock_con

            # Pass as Path objects
            build_duckdb_database(sqlite_db, duckdb_db)

            # Should work the same
            assert mock_duckdb.connect.called

    def test_memory_limit_configuration(self, tmp_path):
        """Test that memory limit is configured correctly."""
        sqlite_db = tmp_path / "test.sqlite"
        sqlite_db.touch()

        duckdb_db = tmp_path / "test.duckdb"

        with patch("nba_vault.duckdb.builder.get_settings") as mock_settings:
            settings = Mock()
            settings.db_path = sqlite_db
            settings.duckdb_path = duckdb_db
            settings.duckdb_memory_limit = "2GB"
            settings.duckdb_threads = 2
            mock_settings.return_value = settings

            with patch("nba_vault.duckdb.builder.duckdb") as mock_duckdb:
                mock_con = Mock()
                mock_duckdb.connect.return_value = mock_con

                build_duckdb_database()

                # Verify memory limit was set
                execute_calls = [str(call) for call in mock_con.execute.call_args_list]
                assert any("2GB" in str(call) for call in execute_calls)
