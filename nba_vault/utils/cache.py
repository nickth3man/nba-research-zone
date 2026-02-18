"""Content-addressable cache for API responses."""

import hashlib
import json
from pathlib import Path
from typing import Any

import structlog

from nba_vault.utils.config import get_settings

logger = structlog.get_logger(__name__)


class ContentCache:
    """Content-addressable cache for API responses."""

    def __init__(self, cache_dir: Path | None = None):
        """
        Initialize cache.

        Args:
            cache_dir: Directory for cache storage. If None, uses settings.
        """
        settings = get_settings()
        self.cache_dir = Path(cache_dir or settings.cache_dir)
        self.enabled = settings.cache_enabled
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_hash(self, key: str) -> str:
        """
        Get content hash for a key.

        Args:
            key: Cache key (typically URL or request identifier).

        Returns:
            SHA256 hash of the key.
        """
        return hashlib.sha256(key.encode()).hexdigest()

    def _get_cache_path(self, key: str) -> Path:
        """
        Get cache file path for a key.

        Args:
            key: Cache key.

        Returns:
            Path to cache file.
        """
        hash_str = self._get_hash(key)
        # Use first 2 chars as subdirectory for better filesystem performance
        subdir = hash_str[:2]
        return self.cache_dir / subdir / f"{hash_str[2:]}.json"

    def get(self, key: str) -> Any | None:
        """
        Get cached response if available.

        Args:
            key: Cache key.

        Returns:
            Cached data if found and valid, None otherwise.
        """
        if not self.enabled:
            return None

        cache_path = self._get_cache_path(key)

        if not cache_path.exists():
            return None

        try:
            with cache_path.open(encoding="utf-8") as f:
                data = json.load(f)
            logger.debug("Cache hit", key=key)
            return data
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Cache file corrupted", key=key, error=str(e))
            return None

    def set(self, key: str, value: Any) -> None:
        """
        Store response in cache.

        Args:
            key: Cache key.
            value: Data to cache (must be JSON-serializable).
        """
        if not self.enabled:
            return

        cache_path = self._get_cache_path(key)
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with cache_path.open("w", encoding="utf-8") as f:
                json.dump(value, f)
            logger.debug("Cached response", key=key)
        except (TypeError, OSError) as e:
            logger.warning("Failed to cache response", key=key, error=str(e))

    def clear(self) -> None:
        """Clear all cached data."""
        if not self.cache_dir.exists():
            return

        for path in self.cache_dir.rglob("*.json"):
            path.unlink()

        logger.info("Cache cleared")

    def stats(self) -> dict[str, int]:
        """
        Get cache statistics.

        Returns:
            Dictionary with cache stats.
        """
        total_files = 0
        total_size = 0

        if self.cache_dir.exists():
            for path in self.cache_dir.rglob("*.json"):
                total_files += 1
                total_size += path.stat().st_size

        return {
            "files": total_files,
            "size_bytes": total_size,
            "size_mb": round(total_size / (1024 * 1024), 2),
        }
