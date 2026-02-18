"""Tests for utility functions."""

import pytest


def test_settings_validation():
    """Test settings validation."""
    from nba_vault.utils.config import Settings

    # Valid settings
    settings = Settings(
        db_path="test.db",
        log_level="INFO",
        log_format="json",
    )
    assert settings.log_level == "INFO"
    assert settings.log_format == "json"

    # Invalid log level
    with pytest.raises(ValueError):
        Settings(log_level="INVALID")

    # Invalid log format
    with pytest.raises(ValueError):
        Settings(log_format="invalid")


def test_settings_caching():
    """Test that settings are cached."""
    from nba_vault.utils.config import get_settings

    # Clear cache first
    get_settings.cache_clear()

    settings1 = get_settings()
    settings2 = get_settings()

    # Should return the same instance
    assert settings1 is settings2


def test_ensure_directories(tmp_path):
    """Test directory creation."""
    from unittest.mock import patch

    from nba_vault.utils.config import Settings, ensure_directories

    # Create settings with temp paths
    settings = Settings(
        db_path=str(tmp_path / "test.db"),
        cache_dir=str(tmp_path / "cache"),
        log_dir=str(tmp_path / "logs"),
    )

    # Patch get_settings to return our test settings
    with patch("nba_vault.utils.config.get_settings", return_value=settings):
        ensure_directories()

        # Check directories were created
        assert (tmp_path / "cache").exists()
        assert (tmp_path / "logs").exists()
        assert tmp_path.exists()


def test_audit_logger(db_connection):
    """Test audit logging functionality."""
    from nba_vault.ingestion.audit import AuditLogger

    audit = AuditLogger(db_connection)

    # Test logging
    audit.log(
        entity_type="test_entity",
        entity_id="test_123",
        source="test_source",
        status="SUCCESS",
        row_count=10,
    )

    # Test retrieval
    status = audit.get_status("test_entity", "test_123")
    assert status is not None
    assert status["entity_type"] == "test_entity"
    assert status["entity_id"] == "test_123"
    assert status["status"] == "SUCCESS"
    assert status["row_count"] == 10

    # Test stats
    stats = audit.get_stats()
    assert "test_entity" in stats
    assert "SUCCESS" in stats["test_entity"]
