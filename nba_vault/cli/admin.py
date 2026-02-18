"""Admin commands: init, migrate, status, validate."""

from pathlib import Path

import structlog
import typer

from nba_vault.schema.connection import get_db_connection, init_database
from nba_vault.schema.migrations import rollback_migration
from nba_vault.utils.config import get_settings

admin_app = typer.Typer(help="Database administration commands.")

logger = structlog.get_logger(__name__)


@admin_app.command()
def init(
    db_path: Path = typer.Option(
        None,
        "--db-path",
        help="Path to SQLite database file",
        envvar="DB_PATH",
    ),
) -> None:
    """
    Initialize the database schema.

    Creates the database file and runs all pending migrations.
    """
    logger.info("Initializing database", db_path=str(db_path))
    try:
        init_database(db_path)
        typer.echo(f"[OK] Database initialized at {db_path or 'nba.sqlite'}")
    except Exception as e:
        logger.error("Failed to initialize database", error=str(e))
        typer.echo(f"[FAIL] Failed to initialize database: {e}", err=True)
        raise typer.Exit(code=1) from e


@admin_app.command()
def migrate(
    rollback: bool = typer.Option(
        False,
        "--rollback",
        "-r",
        help="Rollback the most recent migration",
    ),
    steps: int = typer.Option(
        1,
        "--steps",
        "-n",
        help="Number of migrations to rollback",
    ),
) -> None:
    """
    Run database migrations.

    Applies pending migrations by default. Use --rollback to undo migrations.
    """
    try:
        if rollback:
            logger.info("Rolling back migrations", steps=steps)
            rollback_migration(steps=steps)
            typer.echo(f"[OK] Rolled back {steps} migration(s)")
        else:
            from nba_vault.schema.migrations import run_migrations

            logger.info("Running migrations")
            run_migrations()
            typer.echo("[OK] Migrations applied successfully")
    except Exception as e:
        logger.error("Migration failed", error=str(e))
        typer.echo(f"[FAIL] Migration failed: {e}", err=True)
        raise typer.Exit(code=1) from e


@admin_app.command()
def status() -> None:
    """Show database status and statistics."""
    settings = get_settings()
    db_path = Path(settings.db_path)

    if not db_path.exists():
        typer.echo("Database not found. Run 'nba-vault admin init' to create it.")
        raise typer.Exit(code=1)

    try:
        conn = get_db_connection()
    except RuntimeError as e:
        logger.error("Cannot open database", error=str(e))
        typer.echo(f"[FAIL] Cannot open database: {e}", err=True)
        raise typer.Exit(code=1) from e

    try:
        cursor = conn.execute(
            """
            SELECT t.name
            FROM sqlite_master AS t
            WHERE t.type = 'table'
                AND t.name NOT LIKE 'sqlite_%'
                AND t.name NOT LIKE '_yoyo_%'
            ORDER BY t.name
            """
        )

        table_names = [row[0] for row in cursor.fetchall()]
        typer.echo(f"\nDatabase Status: {db_path}")
        typer.echo(f"   Size: {db_path.stat().st_size / (1024 * 1024):.1f} MB\n")
        typer.echo("Tables:")

        for table_name in table_names:
            # Quote identifier to prevent SQL injection from unexpected table names
            count_cursor = conn.execute(
                f'SELECT COUNT(*) FROM "{table_name}"'  # noqa: S608
            )
            count = count_cursor.fetchone()[0]
            status_str = f"{count:,} rows" if count > 0 else "(empty)"
            typer.echo(f"  - {table_name}: {status_str}")

        audit_cursor = conn.execute(
            """
            SELECT
                entity_type,
                status,
                COUNT(*) as count
            FROM ingestion_audit
            GROUP BY entity_type, status
            ORDER BY entity_type, status
            """
        )

        typer.echo("\nIngestion Status:")
        for row in audit_cursor.fetchall():
            entity_type, status_val, count = row
            emoji = "[OK]" if status_val == "SUCCESS" else "[FAIL]"
            typer.echo(f"  {emoji} {entity_type}: {count:,} {status_val}")

    except Exception as e:
        logger.error("Failed to get status", error=str(e))
        typer.echo(f"[FAIL] Failed to get status: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()


@admin_app.command()
def validate(
    checks: list[str] = typer.Option(
        None,
        "--check",
        "-c",
        help="Specific validation checks to run (default: all)",
    ),
) -> None:
    """
    Validate database integrity and data completeness.

    Runs all validation checks by default. Use --check to run specific checks.
    Available checks: fk_integrity, game_coverage, data_availability, schema_version.
    """
    logger.info("Running validation", checks=checks or ["all"])

    try:
        conn = get_db_connection()
    except RuntimeError as e:
        logger.error("Cannot open database", error=str(e))
        typer.echo(f"[FAIL] Cannot open database: {e}", err=True)
        raise typer.Exit(code=1) from e

    try:
        # TODO: Implement validation logic
        typer.echo("[FAIL] Validation not yet implemented", err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        logger.error("Validation failed", error=str(e))
        typer.echo(f"[FAIL] Validation failed: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()
