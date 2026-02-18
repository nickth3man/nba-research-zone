"""Tests for ingestion framework."""

import pytest
from unittest.mock import Mock, patch


def test_base_ingestor_interface():
    """Test that BaseIngestor provides the required interface."""
    from nba_vault.ingestion.base import BaseIngestor
    from abc import ABC, abstractmethod

    # Check that BaseIngestor is abstract
    assert issubclass(BaseIngestor, ABC)

    # Check that required methods are abstract
    assert hasattr(BaseIngestor, "fetch")
    assert hasattr(BaseIngestor, "validate")
    assert hasattr(BaseIngestor, "upsert")
    assert hasattr(BaseIngestor, "ingest")


def test_ingestor_registry():
    """Test ingestor registry functionality."""
    from nba_vault.ingestion.registry import (
        register_ingestor,
        get_ingestor,
        list_ingestors,
        create_ingestor,
    )
    from nba_vault.ingestion.base import BaseIngestor

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
    import time

    limiter = RateLimiter(rate=5, per=1.0)

    # Should allow first 5 requests immediately
    for _ in range(5):
        assert limiter.acquire(block=False) is True

    # 6th request should be blocked without blocking
    assert limiter.acquire(block=False) is False


def test_content_cache():
    """Test content cache functionality."""
    from nba_vault.utils.cache import ContentCache
    import tempfile
    from pathlib import Path

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
