"""Export commands: export."""

from pathlib import Path

import structlog
import typer

from nba_vault.duckdb.builder import build_duckdb_database

export_app = typer.Typer(help="Database export commands.")

logger = structlog.get_logger(__name__)


@export_app.command()
def export(
    format: str = typer.Option(
        "parquet",
        "--format",
        "-f",
        help="Export format: parquet, csv, or duckdb",
    ),
    output_dir: Path = typer.Option(
        Path("exports"),
        "--output-dir",
        "-o",
        help="Output directory for exported files",
    ),
    entities: list[str] = typer.Option(
        None,
        "--entity",
        "-e",
        help="Specific entities to export (default: all)",
    ),
) -> None:
    """
    Export database to various formats.

    Exports all tables by default. Use --entity to export specific tables.
    """
    logger.info(
        "Starting export",
        format=format,
        output_dir=str(output_dir),
        entities=entities or ["all"],
    )

    try:
        if format == "duckdb":
            typer.echo("Building DuckDB analytical database...")
            build_duckdb_database()
            typer.echo("[OK] DuckDB database built successfully")
        else:
            # TODO: Implement export logic for other formats
            typer.echo(f"[FAIL] Export format '{format}' not yet implemented", err=True)
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error("Export failed", error=str(e))
        typer.echo(f"[FAIL] Export failed: {e}", err=True)
        raise typer.Exit(code=1) from e
