"""Configuration management using environment variables."""

from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings

# Load environment variables from .env file
load_dotenv()


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Database Configuration
    db_path: str = Field(default="nba.sqlite", description="Path to SQLite database")
    duckdb_path: str = Field(default="nba.duckdb", description="Path to DuckDB database")

    # API Rate Limiting
    nba_api_rate_limit: int = Field(default=8, description="Requests per minute to NBA API")
    nba_api_retry_attempts: int = Field(default=5, description="Number of retry attempts")
    nba_api_retry_delay: int = Field(default=30, description="Initial retry delay in seconds")

    # Cache Configuration
    cache_dir: str = Field(default="cache", description="Directory for cached responses")
    cache_enabled: bool = Field(default=True, description="Enable response caching")

    # Logging
    log_level: str = Field(default="INFO", description="Logging level")
    log_format: str = Field(default="json", description="Log format (json or console)")
    log_dir: str = Field(default="logs", description="Directory for log files")

    # Ingestion Configuration
    backfill_workers: int = Field(default=1, description="Number of parallel workers")
    backfill_start_season: int = Field(default=1946, description="Start season for backfill")
    backfill_end_season: int = Field(default=2024, description="End season for backfill")

    # Scheduling
    enable_scheduler: bool = Field(default=False, description="Enable automatic scheduling")
    schedule_interval: str = Field(default="daily", description="Schedule interval")
    schedule_time: str = Field(default="02:00", description="Schedule time (HH:MM)")

    # DuckDB Configuration
    duckdb_memory_limit: str = Field(default="4GB", description="DuckDB memory limit")
    duckdb_threads: int = Field(default=4, description="DuckDB thread count")

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is a valid option."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return v.upper()

    @field_validator("log_format")
    @classmethod
    def validate_log_format(cls, v: str) -> str:
        """Validate log format is a valid option."""
        if v not in {"json", "console"}:
            raise ValueError("log_format must be 'json' or 'console'")
        return v

    class Config:
        """Pydantic configuration."""

        env_prefix = ""
        case_sensitive = False
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Returns:
        Settings: Application settings.
    """
    return Settings()


def ensure_directories() -> None:
    """
    Ensure all required directories exist.

    This creates the cache, logs, and database directories if they don't exist.
    """
    settings = get_settings()

    for path_key in ["cache_dir", "log_dir"]:
        path = getattr(settings, path_key)
        Path(path).mkdir(parents=True, exist_ok=True)

    # Ensure database parent directory exists
    db_path = Path(settings.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
