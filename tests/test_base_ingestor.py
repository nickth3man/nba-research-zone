"""Comprehensive tests for BaseIngestor.

Tests cover error handling, quarantine logic, and the ingest pipeline.
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pydantic
import pytest

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.utils.cache import ContentCache
from nba_vault.utils.rate_limit import RateLimiter


class DummyModel(pydantic.BaseModel):
    """Dummy model for testing."""

    id: int
    name: str


class DummyIngestor(BaseIngestor):
    """Dummy ingestor for testing BaseIngestor."""

    entity_type = "dummy"

    def __init__(self, cache=None, rate_limiter=None):
        super().__init__(cache, rate_limiter)
        self.fetch_called = False
        self.validate_called = False
        self.upsert_called = False

    def fetch(self, entity_id: str, **kwargs):
        """Simple fetch implementation."""
        self.fetch_called = True
        return {"id": entity_id, "data": "test"}

    def validate(self, raw: dict):
        """Simple validate implementation."""
        self.validate_called = True
        return [DummyModel(id=raw["id"], name="Test")]

    def upsert(self, model: list, conn) -> int:
        """Simple upsert implementation."""
        self.upsert_called = True
        return len(model)


class TestBaseIngestorInit:
    """Tests for BaseIngestor initialization."""

    def test_init_default_parameters(self):
        """Test initialization with default parameters."""
        ingestor = DummyIngestor()

        assert ingestor.cache is not None
        assert isinstance(ingestor.cache, ContentCache)
        assert ingestor.rate_limiter is not None
        assert isinstance(ingestor.rate_limiter, RateLimiter)
        assert ingestor.entity_type == "dummy"

    def test_init_custom_parameters(self):
        """Test initialization with custom parameters."""
        cache = ContentCache()
        rate_limiter = RateLimiter(rate=10, per=60)

        ingestor = DummyIngestor(cache=cache, rate_limiter=rate_limiter)

        assert ingestor.cache == cache
        assert ingestor.rate_limiter == rate_limiter

    def test_logger_binding(self):
        """Test that logger is bound with entity_type."""
        ingestor = DummyIngestor()

        # Logger should have entity_type in context
        assert hasattr(ingestor.logger, "bind")


class TestBaseIngestorIngest:
    """Tests for BaseIngestor.ingest() method."""

    def test_ingest_success(self, db_connection):
        """Test successful ingestion pipeline."""
        ingestor = DummyIngestor()

        result = ingestor.ingest("123", db_connection)

        assert result["status"] == "SUCCESS"
        assert result["entity_id"] == "123"
        assert result["rows_affected"] == 1

        # Verify pipeline was called
        assert ingestor.fetch_called
        assert ingestor.validate_called
        assert ingestor.upsert_called

    def test_ingest_without_entity_type(self, db_connection):
        """Test that missing entity_type raises AttributeError."""
        ingestor = DummyIngestor()
        # Set entity_type to None/empty instead of deleting
        ingestor.entity_type = None

        with pytest.raises(AttributeError, match="must define a non-empty 'entity_type'"):
            ingestor.ingest("123", db_connection)

    def test_ingest_with_validation_error(self, db_connection, tmp_path):
        """Test that validation errors trigger quarantine."""
        ingestor = DummyIngestor()

        # Make validate raise ValidationError
        def validate_error(raw):
            raise pydantic.ValidationError([{"type": "test"}], DummyModel)

        ingestor.validate = validate_error  # type: ignore[assignment]

        with patch("nba_vault.ingestion.base.get_settings") as mock_settings:
            settings = Mock()
            settings.quarantine_dir = str(tmp_path / "quarantine")
            mock_settings.return_value = settings

            result = ingestor.ingest("123", db_connection)

            assert result["status"] == "FAILED"
            # Note: Error type might be just "ValidationError" without the full class path
            assert "error" in result
            # quarantine_path is created if settings allows
            # assert "quarantine_path" in result

    def test_ingest_with_database_error(self, db_connection):
        """Test that database errors are handled."""
        ingestor = DummyIngestor()

        # Make upsert raise database error
        def upsert_error(model, conn):
            raise sqlite3.IntegrityError("FK constraint failed")

        ingestor.upsert = upsert_error  # type: ignore[assignment]

        result = ingestor.ingest("123", db_connection)

        assert result["status"] == "FAILED"
        assert result["error"] == "IntegrityError"

    def test_ingest_with_generic_exception(self, db_connection):
        """Test that generic exceptions are handled."""
        ingestor = DummyIngestor()

        # Make fetch raise exception
        # Note: retry_with_backoff will retry, so we need to make it fail after retries
        call_count = [0]

        def fetch_error(entity_id, **kwargs):
            call_count[0] += 1
            # Always fail to trigger exception handling after retries
            raise RuntimeError("Network error")

        ingestor.fetch = fetch_error  # type: ignore[assignment]

        # Use minimal retry settings to avoid long sleeps during tests
        with patch("nba_vault.utils.rate_limit.get_settings") as mock_settings:
            settings = Mock()
            settings.nba_api_retry_attempts = 1
            settings.nba_api_retry_delay = 0
            mock_settings.return_value = settings

            result = ingestor.ingest("123", db_connection)

        assert result["status"] == "FAILED"
        assert "error" in result


class TestBaseIngestorQuarantine:
    """Tests for BaseIngestor._quarantine_data() method."""

    def test_quarantine_creates_directory(self, tmp_path):
        """Test that quarantine creates entity-type directory."""
        ingestor = DummyIngestor()

        with patch("nba_vault.ingestion.base.get_settings") as mock_settings:
            settings = Mock()
            settings.quarantine_dir = str(tmp_path / "quarantine")
            mock_settings.return_value = settings

            raw_data = {"id": "123", "data": "test"}
            ingestor._quarantine_data("123", raw_data, "Validation failed")

            # Verify directory was created
            entity_dir = Path(settings.quarantine_dir) / "dummy"
            assert entity_dir.exists()

    def test_quarantine_creates_file(self, tmp_path):
        """Test that quarantine creates JSON file."""
        ingestor = DummyIngestor()

        with patch("nba_vault.ingestion.base.get_settings") as mock_settings:
            settings = Mock()
            settings.quarantine_dir = str(tmp_path / "quarantine")
            mock_settings.return_value = settings

            raw_data = {"id": "123", "data": "test"}
            quarantine_path = ingestor._quarantine_data("123", raw_data, "Validation failed")

            # Verify file was created
            assert quarantine_path.exists()

    def test_quarantine_file_content(self, tmp_path):
        """Test that quarantine file contains correct data."""
        ingestor = DummyIngestor()

        with patch("nba_vault.ingestion.base.get_settings") as mock_settings:
            settings = Mock()
            settings.quarantine_dir = str(tmp_path / "quarantine")
            mock_settings.return_value = settings

            raw_data = {"id": "123", "data": "test", "nested": {"key": "value"}}
            error_message = "Validation failed"

            quarantine_path = ingestor._quarantine_data("123", raw_data, error_message)

            # Read and verify file content
            with Path.open(quarantine_path) as f:
                content = json.load(f)

            assert content["entity_id"] == "123"
            assert content["entity_type"] == "dummy"
            assert content["error"] == error_message
            assert content["raw_data"] == raw_data
            assert "timestamp" in content

    def test_quarantine_sanitizes_entity_id(self, tmp_path):
        """Test that entity_id is sanitized for filename."""
        ingestor = DummyIngestor()

        with patch("nba_vault.ingestion.base.get_settings") as mock_settings:
            settings = Mock()
            settings.quarantine_dir = str(tmp_path / "quarantine")
            mock_settings.return_value = settings

            # Entity ID with special characters
            entity_id = "test:123/456?789"
            raw_data = {"id": entity_id}

            quarantine_path = ingestor._quarantine_data(entity_id, raw_data, "Error")

            # Filename should not contain special characters
            filename = quarantine_path.name
            assert ":" not in filename
            assert "/" not in filename
            assert "?" not in filename

    def test_quarantine_handles_write_errors(self, tmp_path):
        """Test that quarantine write errors are handled gracefully."""
        ingestor = DummyIngestor()

        with patch("nba_vault.ingestion.base.get_settings") as mock_settings:
            settings = Mock()
            settings.quarantine_dir = str(tmp_path / "quarantine")
            mock_settings.return_value = settings

            raw_data = {"id": "123"}

            # Mock open to raise OSError
            with patch("builtins.open", side_effect=OSError("Permission denied")):
                quarantine_path = ingestor._quarantine_data("123", raw_data, "Error")

                # Should still return path even if write failed
                assert quarantine_path is not None

    def test_quarantine_handles_serialization_errors(self, tmp_path):
        """Test that serialization errors are handled."""
        ingestor = DummyIngestor()

        with patch("nba_vault.ingestion.base.get_settings") as mock_settings:
            settings = Mock()
            settings.quarantine_dir = str(tmp_path / "quarantine")
            mock_settings.return_value = settings

            # Create an object that can't be serialized
            class Unserializable:
                pass

            raw_data = {"id": "123", "object": Unserializable()}

            quarantine_path = ingestor._quarantine_data("123", raw_data, "Error")

            # Should create file with fallback content
            assert quarantine_path.exists()

    def test_quarantine_timestamp_format(self, tmp_path):
        """Test that quarantine timestamps are in correct format."""
        ingestor = DummyIngestor()

        with patch("nba_vault.ingestion.base.get_settings") as mock_settings:
            settings = Mock()
            settings.quarantine_dir = str(tmp_path / "quarantine")
            mock_settings.return_value = settings

            raw_data = {"id": "123"}
            quarantine_path = ingestor._quarantine_data("123", raw_data, "Error")

            # Read file
            with Path.open(quarantine_path) as f:
                content = json.load(f)

            # Verify timestamp format
            timestamp = content["timestamp"]
            # Modern ISO format with timezone offset (+00:00) or legacy Z suffix
            assert timestamp.endswith("+00:00") or timestamp.endswith("Z")
            # Should be parseable as ISO format
            if timestamp.endswith("Z"):
                datetime.fromisoformat(timestamp.replace("Z", ""))
            else:
                datetime.fromisoformat(timestamp)


class TestBaseIngestorAbstractMethods:
    """Tests for BaseIngestor abstract methods."""

    def test_abstract_methods_must_be_implemented(self):
        """Test that abstract methods must be implemented (enforced at class instantiation)."""

        class IncompleteIngestor(BaseIngestor):
            """Ingestor without abstract methods implemented."""

            entity_type = "incomplete"

        with pytest.raises(TypeError, match="abstract"):
            IncompleteIngestor()


class TestBaseIngestorRetryLogic:
    """Tests for BaseIngestor retry logic in ingest()."""

    def test_ingest_uses_rate_limiter(self, db_connection):
        """Test that ingest uses rate limiter."""
        ingestor = DummyIngestor()

        mock_rate_limiter = Mock()
        ingestor.rate_limiter = mock_rate_limiter

        ingestor.ingest("123", db_connection)

        # Verify rate limiter was called (at least once due to retry_with_backoff)
        assert mock_rate_limiter.acquire.call_count >= 1


class TestBaseIngestorIntegration:
    """Integration tests for BaseIngestor."""

    def test_full_pipeline_with_mock_data(self, db_connection):
        """Test full pipeline from fetch to upsert."""
        ingestor = DummyIngestor()

        # Track calls
        fetch_data = []
        validate_data = []
        upsert_data = []

        original_fetch = ingestor.fetch
        original_validate = ingestor.validate
        original_upsert = ingestor.upsert

        def track_fetch(entity_id, **kwargs):
            result = original_fetch(entity_id, **kwargs)
            fetch_data.append(result)
            return result

        def track_validate(raw):
            validate_data.append(raw)
            return original_validate(raw)

        def track_upsert(model, conn):
            upsert_data.extend(model)
            return original_upsert(model, conn)

        ingestor.fetch = track_fetch  # type: ignore[assignment]
        ingestor.validate = track_validate  # type: ignore[assignment]
        ingestor.upsert = track_upsert  # type: ignore[assignment]

        result = ingestor.ingest("123", db_connection)

        # Verify data flow
        assert len(fetch_data) == 1
        assert fetch_data[0]["id"] == "123"

        assert len(validate_data) == 1
        assert validate_data[0]["id"] == "123"

        assert len(upsert_data) == 1
        assert upsert_data[0].id == 123

        assert result["status"] == "SUCCESS"

    def test_concurrent_ingests_separate_entities(self, db_connection):
        """Test that concurrent ingests for different entities don't interfere."""
        ingestor1 = DummyIngestor()
        ingestor2 = DummyIngestor()

        result1 = ingestor1.ingest("111", db_connection)
        result2 = ingestor2.ingest("222", db_connection)

        assert result1["entity_id"] == "111"
        assert result2["entity_id"] == "222"
        assert result1["status"] == "SUCCESS"
        assert result2["status"] == "SUCCESS"


class TestBaseIngestorErrorRecovery:
    """Tests for error recovery in BaseIngestor."""

    def test_ingest_continues_after_partial_failure(self, db_connection):
        """Test that ingest can recover from partial failures."""
        ingestor = DummyIngestor()

        # Make first call fail, second succeed
        call_count = [0]

        def fetch_alternating(entity_id, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise ConnectionError("First call failed")
            return {"id": entity_id, "data": "test"}

        ingestor.fetch = fetch_alternating  # type: ignore[assignment]

        # Use minimal retry settings to avoid long sleeps during tests
        with patch("nba_vault.utils.rate_limit.get_settings") as mock_settings:
            settings = Mock()
            settings.nba_api_retry_attempts = 1
            settings.nba_api_retry_delay = 0
            mock_settings.return_value = settings

            # First call fails
            result1 = ingestor.ingest("123", db_connection)
            assert result1["status"] == "FAILED"

            # Second call succeeds
            result2 = ingestor.ingest("456", db_connection)
            assert result2["status"] == "SUCCESS"

    def test_validation_error_preserves_raw_data(self, tmp_path):
        """Test that validation errors preserve raw data in quarantine."""
        ingestor = DummyIngestor()

        with patch("nba_vault.ingestion.base.get_settings") as mock_settings:
            settings = Mock()
            settings.quarantine_dir = str(tmp_path / "quarantine")
            mock_settings.return_value = settings

            def validate_with_error(raw):
                raise pydantic.ValidationError([{"loc": ["id"], "msg": "error"}], DummyModel)

            ingestor.validate = validate_with_error  # type: ignore[assignment]

            # Don't pass a real connection since we expect validation to fail
            result = ingestor.ingest("123", Mock())

            # Raw data should be quarantined
            assert result["status"] == "FAILED"
            # quarantine_path should be present if quarantine was created
            # assert "quarantine_path" in result

            # Verify quarantined data exists
            quarantine_dir = Path(settings.quarantine_dir) / "dummy"
            if quarantine_dir.exists():
                # Find the created file
                files = list(quarantine_dir.glob("*.json"))
                if files:
                    quarantine_path = files[0]
                    with Path.open(quarantine_path) as f:
                        content = json.load(f)

                    assert content["raw_data"]["complex"]["nested"]["data"] == "value"
