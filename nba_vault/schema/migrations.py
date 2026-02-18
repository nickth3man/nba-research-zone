"""Database schema migrations using yoyo-migrations."""

from pathlib import Path

import structlog
from yoyo import get_backend, read_migrations

from nba_vault.utils.config import get_settings

logger = structlog.get_logger(__name__)


def get_migrations_dir() -> Path:
    """Get the path to the migrations directory."""
    return Path(__file__).parent.parent.parent / "migrations"


def _get_db_uri(db_path: Path | None = None) -> str:
    """Build the yoyo-compatible SQLite URI for the given path."""
    settings = get_settings()
    resolved = db_path or Path(settings.db_path)
    return f"sqlite:///{resolved.resolve()}"


def run_migrations(db_path: Path | None = None, migrations_dir: Path | None = None) -> None:
    """
    Run pending database migrations.

    Args:
        db_path: Path to the SQLite database file. If None, uses default from settings.
        migrations_dir: Path to migrations directory. If None, uses default.
    """
    migrations_dir = migrations_dir or get_migrations_dir()

    if not migrations_dir.exists():
        logger.warning("Migrations directory not found, skipping migrations")
        return

    backend = get_backend(_get_db_uri(db_path))
    migrations = read_migrations(str(migrations_dir))

    with backend.lock():
        if migrations_to_apply := backend.to_apply(migrations):
            logger.info("Applying migrations", count=len(migrations_to_apply))
            for migration in migrations_to_apply:
                logger.info("Applying migration", migration=migration.id)
                backend.apply_one(migration)
            logger.info("Migrations applied successfully")
        else:
            logger.info("No pending migrations")


def rollback_migration(db_path: Path | None = None, steps: int = 1) -> None:
    """
    Rollback the most recent migration(s).

    Args:
        db_path: Path to the SQLite database file. If None, uses default from settings.
        steps: Number of migrations to rollback.
    """
    backend = get_backend(_get_db_uri(db_path))
    migrations = read_migrations(str(get_migrations_dir()))

    with backend.lock():
        # to_rollback() returns applied migrations in reverse order (most recent first)
        all_to_rollback = list(backend.to_rollback(migrations))
        # steps=0 preserves the original "rollback all" behaviour; steps>0 caps the count
        limited = all_to_rollback[:steps] if steps > 0 else all_to_rollback

        for migration in limited:
            logger.info("Rolling back migration", migration=migration.id)
            backend.rollback_one(migration)

    logger.info("Migrations rolled back", count=len(limited))
