"""Database schema migrations using yoyo-migrations."""

from pathlib import Path

import structlog
from yoyo import get_backend, read_migrations

logger = structlog.get_logger(__name__)


def get_migrations_dir() -> Path:
    """Get the path to the migrations directory."""
    return Path(__file__).parent.parent.parent / "migrations"


def run_migrations(conn, migrations_dir: Path | None = None) -> None:
    """
    Run pending database migrations.

    Args:
        conn: SQLite database connection.
        migrations_dir: Path to migrations directory. If None, uses default.
    """
    migrations_dir = migrations_dir or get_migrations_dir()

    if not migrations_dir.exists():
        logger.warning("Migrations directory not found, skipping migrations")
        return

    backend = get_backend(conn)
    migrations = read_migrations(str(migrations_dir))

    if migrations_to_apply := backend.to_apply(migrations):
        logger.info("Applying migrations", count=len(migrations_to_apply))
        for migration in migrations_to_apply:
            logger.info("Applying migration", migration=migration.id)
            backend.apply_one(migration)
        logger.info("Migrations applied successfully")
    else:
        logger.info("No pending migrations")


def rollback_migration(conn, steps: int = 1) -> None:
    """
    Rollback the most recent migration(s).

    Args:
        conn: SQLite database connection.
        steps: Number of migrations to rollback.
    """
    backend = get_backend(conn)
    migrations = read_migrations(str(get_migrations_dir()))

    applied = backend.applied_migrations(migrations)
    to_rollback = applied[-steps:] if len(applied) >= steps else applied

    for migration in reversed(to_rollback):
        logger.info("Rolling back migration", migration=migration.id)
        backend.rollback_one(migration)

    logger.info("Migrations rolled back", count=len(to_rollback))
