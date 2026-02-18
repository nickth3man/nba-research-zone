"""Tests for ingestion framework."""

import sqlite3
from unittest.mock import MagicMock, patch

import pydantic
import pytest

# ---------------------------------------------------------------------------
# BaseIngestor.ingest() pipeline tests
# ---------------------------------------------------------------------------


def _make_concrete_ingestor(fetch_return=None, validate_return=None, upsert_return=1):
    """Build a concrete BaseIngestor subclass for pipeline testing."""
    from nba_vault.ingestion.base import BaseIngestor
    from nba_vault.utils.cache import ContentCache
    from nba_vault.utils.rate_limit import RateLimiter

    class _Ingestor(BaseIngestor):
        entity_type = "test_pipeline"

        def fetch(self, entity_id, **kwargs):
            if fetch_return is Exception or (
                isinstance(fetch_return, type) and issubclass(fetch_return, Exception)
            ):
                raise fetch_return("fetch error")
            if isinstance(fetch_return, Exception):
                raise fetch_return
            return fetch_return or {"items": []}

        def validate(self, raw):
            if validate_return is not None:
                return validate_return
            return []

        def upsert(self, model: list[pydantic.BaseModel], conn) -> int:
            return upsert_return

    cache = ContentCache.__new__(ContentCache)
    cache._cache_dir = None
    limiter = RateLimiter(rate=100, per=1.0)
    return _Ingestor(cache=MagicMock(), rate_limiter=limiter)


def test_ingest_success_pipeline():
    """Happy path: fetch → validate → upsert → SUCCESS result."""
    from nba_vault.ingestion.base import BaseIngestor
    from nba_vault.utils.rate_limit import RateLimiter

    class GoodIngestor(BaseIngestor):
        entity_type = "good"

        def fetch(self, entity_id, **kwargs):
            return {"data": "ok"}

        def validate(self, raw):
            return []

        def upsert(self, model: list[pydantic.BaseModel], conn) -> int:
            return 3

    ingestor = GoodIngestor(cache=MagicMock(), rate_limiter=RateLimiter(rate=100, per=1.0))
    result = ingestor.ingest("eid-1", conn=MagicMock())

    assert result["status"] == "SUCCESS"
    assert result["rows_affected"] == 3
    assert result["entity_id"] == "eid-1"


def test_ingest_validation_error():
    """pydantic.ValidationError during validate() returns FAILED status."""
    from nba_vault.ingestion.base import BaseIngestor
    from nba_vault.utils.rate_limit import RateLimiter

    class BadValidateIngestor(BaseIngestor):
        entity_type = "bad_validate"

        def fetch(self, entity_id, **kwargs):
            return {}

        def validate(self, raw):
            # Trigger a real ValidationError
            class Strict(pydantic.BaseModel):
                required_field: int

            Strict(required_field="not_an_int")  # type: ignore[arg-type]

        def upsert(self, model: list[pydantic.BaseModel], conn) -> int:
            return 0

    ingestor = BadValidateIngestor(cache=MagicMock(), rate_limiter=RateLimiter(rate=100, per=1.0))
    result = ingestor.ingest("eid-2", conn=MagicMock())

    assert result["status"] == "FAILED"
    assert result["error"] == "ValidationError"


def test_ingest_sqlite_error():
    """sqlite3.Error during upsert() returns FAILED status."""
    from nba_vault.ingestion.base import BaseIngestor
    from nba_vault.utils.rate_limit import RateLimiter

    class SqliteErrorIngestor(BaseIngestor):
        entity_type = "sqlite_error"

        def fetch(self, entity_id, **kwargs):
            return {}

        def validate(self, raw):
            return []

        def upsert(self, model: list[pydantic.BaseModel], conn) -> int:
            raise sqlite3.OperationalError("no such table")

    ingestor = SqliteErrorIngestor(cache=MagicMock(), rate_limiter=RateLimiter(rate=100, per=1.0))
    result = ingestor.ingest("eid-3", conn=MagicMock())

    assert result["status"] == "FAILED"
    assert result["error"] == "OperationalError"


def test_ingest_generic_exception():
    """Unexpected exceptions during fetch() return FAILED status."""
    from nba_vault.ingestion.base import BaseIngestor
    from nba_vault.utils.rate_limit import RateLimiter

    class ExplodingIngestor(BaseIngestor):
        entity_type = "exploding"

        def fetch(self, entity_id, **kwargs):
            raise ConnectionError("network down")

        def validate(self, raw):
            return []

        def upsert(self, model: list[pydantic.BaseModel], conn) -> int:
            return 0

    ingestor = ExplodingIngestor(cache=MagicMock(), rate_limiter=RateLimiter(rate=100, per=1.0))
    result = ingestor.ingest("eid-4", conn=MagicMock())

    assert result["status"] == "FAILED"
    assert result["error"] == "ConnectionError"


def test_ingest_missing_entity_type():
    """AttributeError is raised when entity_type is not set."""
    from nba_vault.ingestion.base import BaseIngestor
    from nba_vault.utils.rate_limit import RateLimiter

    class NoEntityType(BaseIngestor):
        entity_type = ""  # empty string → falsy

        def fetch(self, entity_id, **kwargs):
            return {}

        def validate(self, raw):
            return []

        def upsert(self, model: list[pydantic.BaseModel], conn) -> int:
            return 0

    ingestor = NoEntityType(cache=MagicMock(), rate_limiter=RateLimiter(rate=100, per=1.0))
    with pytest.raises(AttributeError, match="entity_type"):
        ingestor.ingest("eid-5", conn=MagicMock())


# ---------------------------------------------------------------------------
# retry_with_backoff tests
# ---------------------------------------------------------------------------


def test_retry_with_backoff_success():
    """Returns result immediately when the function succeeds first try."""
    from nba_vault.utils.rate_limit import retry_with_backoff

    result = retry_with_backoff(lambda: 42, max_attempts=3, base_delay=0)
    assert result == 42


def test_retry_with_backoff_succeeds_on_retry():
    """Succeeds on second attempt after first raises."""
    from nba_vault.utils.rate_limit import retry_with_backoff

    call_count = {"n": 0}

    def flaky():
        call_count["n"] += 1
        if call_count["n"] < 2:
            raise ValueError("transient")
        return "ok"

    with patch("time.sleep"):  # don't actually sleep
        result = retry_with_backoff(flaky, max_attempts=3, base_delay=0)

    assert result == "ok"
    assert call_count["n"] == 2


def test_retry_with_backoff_exhausted():
    """Raises after all attempts fail."""
    from nba_vault.utils.rate_limit import retry_with_backoff

    with patch("time.sleep"), pytest.raises(RuntimeError, match="always fails"):
        retry_with_backoff(
            lambda: (_ for _ in ()).throw(RuntimeError("always fails")),
            max_attempts=2,
            base_delay=0,
        )


def test_base_ingestor_interface():
    """Test that BaseIngestor provides the required interface."""
    from abc import ABC

    from nba_vault.ingestion.base import BaseIngestor

    # Check that BaseIngestor is abstract
    assert issubclass(BaseIngestor, ABC)

    # Check that required methods are abstract
    assert hasattr(BaseIngestor, "fetch")
    assert hasattr(BaseIngestor, "validate")
    assert hasattr(BaseIngestor, "upsert")
    assert hasattr(BaseIngestor, "ingest")


def test_ingestor_registry():
    """Test ingestor registry functionality."""
    from nba_vault.ingestion.base import BaseIngestor
    from nba_vault.ingestion.registry import (
        create_ingestor,
        get_ingestor,
        list_ingestors,
        register_ingestor,
    )

    # Create a test ingestor
    @register_ingestor
    class TestIngestor(BaseIngestor):
        entity_type = "test"

        def fetch(self, entity_id, **kwargs):
            return {"test": "data"}

        def validate(self, raw):
            from pydantic import BaseModel

            class TestModel(BaseModel):
                test: str

            return TestModel(**raw)

        def upsert(self, model, conn):
            return 1

    # Check registration
    assert "test" in list_ingestors()
    assert get_ingestor("test") == TestIngestor

    # Check creation
    ingestor = create_ingestor("test")
    assert isinstance(ingestor, TestIngestor)

    # Check non-existent ingestor
    assert get_ingestor("nonexistent") is None
    assert create_ingestor("nonexistent") is None


def test_rate_limiter():
    """Test rate limiter functionality."""

    from nba_vault.utils.rate_limit import RateLimiter

    limiter = RateLimiter(rate=5, per=1.0)

    # Should allow first 5 requests immediately
    for _ in range(5):
        assert limiter.acquire(block=False) is True

    # 6th request should be blocked without blocking
    assert limiter.acquire(block=False) is False


def test_content_cache():
    """Test content cache functionality."""
    import tempfile
    from pathlib import Path

    from nba_vault.utils.cache import ContentCache

    with tempfile.TemporaryDirectory() as tmpdir:
        cache = ContentCache(cache_dir=Path(tmpdir))

        # Test cache miss
        result = cache.get("test_key")
        assert result is None

        # Test cache set and get
        data = {"test": "data", "number": 123}
        cache.set("test_key", data)
        result = cache.get("test_key")
        assert result == data

        # Test cache stats
        stats = cache.stats()
        assert stats["files"] == 1
        assert stats["size_bytes"] > 0


def test_quarantine_on_validation_error():
    """Test that failed validation data is quarantined."""
    import json
    import tempfile
    from pathlib import Path
    from unittest.mock import patch

    from nba_vault.ingestion.base import BaseIngestor
    from nba_vault.utils.rate_limit import RateLimiter

    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch("nba_vault.ingestion.base.get_settings") as mock_settings,
    ):
        mock_settings.return_value.quarantine_dir = tmpdir

        class QuarantineTestIngestor(BaseIngestor):
            entity_type = "quarantine_test"

            def fetch(self, entity_id, **kwargs):
                return {"items": [{"bad_data": "should_fail"}], "entity_id": entity_id}

            def validate(self, raw):
                # Trigger validation error
                class StrictModel(pydantic.BaseModel):
                    bad_data: int  # Will fail with string value

                return [StrictModel(**item) for item in raw["items"]]

            def upsert(self, model: list[pydantic.BaseModel], conn) -> int:
                return 0

        ingestor = QuarantineTestIngestor(
            cache=MagicMock(), rate_limiter=RateLimiter(rate=100, per=1.0)
        )
        result = ingestor.ingest("test-entity-123", conn=MagicMock())

        # Check that ingestion failed
        assert result["status"] == "FAILED"
        assert result["error"] == "ValidationError"
        assert "quarantine_path" in result

        # Verify quarantine file was created
        quarantine_path = Path(result["quarantine_path"])
        assert quarantine_path.exists()
        assert quarantine_path.parent.parent == Path(tmpdir)
        assert quarantine_path.parent.name == "quarantine_test"

        # Verify quarantine file contents
        with quarantine_path.open(encoding="utf-8") as f:
            quarantine_data = json.load(f)

        assert quarantine_data["entity_id"] == "test-entity-123"
        assert quarantine_data["entity_type"] == "quarantine_test"
        assert "timestamp" in quarantine_data
        assert "error" in quarantine_data
        assert quarantine_data["raw_data"]["items"][0]["bad_data"] == "should_fail"


def test_quarantine_sanitizes_entity_id():
    """Test that entity IDs with special characters are sanitized in filenames."""
    import tempfile
    from pathlib import Path
    from unittest.mock import patch

    from nba_vault.ingestion.base import BaseIngestor
    from nba_vault.utils.rate_limit import RateLimiter

    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch("nba_vault.ingestion.base.get_settings") as mock_settings,
    ):
        mock_settings.return_value.quarantine_dir = tmpdir

        class SanitizeTestIngestor(BaseIngestor):
            entity_type = "sanitize_test"

            def fetch(self, entity_id, **kwargs):
                # Return data that will fail validation
                return {"bad_field": "not_an_int"}

            def validate(self, raw):
                # Trigger a real validation error
                class StrictModel(pydantic.BaseModel):
                    bad_field: int

                return StrictModel(**raw)

            def upsert(self, model: list[pydantic.BaseModel], conn) -> int:
                return 0

        ingestor = SanitizeTestIngestor(
            cache=MagicMock(), rate_limiter=RateLimiter(rate=100, per=1.0)
        )
        result = ingestor.ingest("entity/with:special*chars?", conn=MagicMock())

        # Verify file was created and name is sanitized
        quarantine_path = Path(result["quarantine_path"])
        assert quarantine_path.exists()
        assert "/" not in quarantine_path.name
        assert ":" not in quarantine_path.name
        assert "*" not in quarantine_path.name
        assert "?" not in quarantine_path.name
        # Should have underscores instead
        assert "entity_with_special_chars_" in quarantine_path.name


def test_quarantine_handles_large_payloads():
    """Test that quarantine handles non-serializable data gracefully."""
    import json
    import tempfile
    from pathlib import Path
    from unittest.mock import patch

    from nba_vault.ingestion.base import BaseIngestor
    from nba_vault.utils.rate_limit import RateLimiter

    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch("nba_vault.ingestion.base.get_settings") as mock_settings,
    ):
        mock_settings.return_value.quarantine_dir = tmpdir

        class LargePayloadIngestor(BaseIngestor):
            entity_type = "large_payload"

            def fetch(self, entity_id, **kwargs):
                # Include a non-serializable object (e.g., a set)
                return {"items": list(range(1000)), "unserializable": {1, 2, 3}}

            def validate(self, raw):
                # Trigger a real validation error by requiring a field that doesn't exist
                class StrictModel(pydantic.BaseModel):
                    required_field: str

                return StrictModel(**raw)

            def upsert(self, model: list[pydantic.BaseModel], conn) -> int:
                return 0

        ingestor = LargePayloadIngestor(
            cache=MagicMock(), rate_limiter=RateLimiter(rate=100, per=1.0)
        )
        result = ingestor.ingest("large-payload", conn=MagicMock())

        # Should still create a quarantine file even with unserializable data
        quarantine_path = Path(result["quarantine_path"])
        assert quarantine_path.exists()

        # File should be valid JSON
        with quarantine_path.open(encoding="utf-8") as f:
            quarantine_data = json.load(f)

        assert quarantine_data["entity_id"] == "large-payload"
        # The set should be converted to a string representation
        assert "unserializable" in quarantine_data["raw_data"]
