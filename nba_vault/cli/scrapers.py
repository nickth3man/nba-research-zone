"""Scraper commands: ingest-injuries, ingest-contracts."""

import structlog
import typer

from nba_vault.schema.connection import get_db_connection

scrapers_app = typer.Typer(help="Web scraping ingestion commands.")

logger = structlog.get_logger(__name__)


@scrapers_app.command(name="ingest-injuries")
def ingest_injuries(
    team: str = typer.Option(
        None,
        "--team",
        "-t",
        help="Team abbreviation (e.g., LAL)",
    ),
    source: str = typer.Option(
        "espn",
        "--source",
        help="Data source: espn, rotowire, or nba",
    ),
) -> None:
    """
    Ingest injury data from various sources.

    Fetches current injury reports from ESPN, Rotowire, or NBA.com.
    Use --team to filter by specific team.
    """
    from nba_vault.ingestion import create_ingestor

    try:
        conn = get_db_connection()
    except RuntimeError as e:
        logger.error("Cannot open database", error=str(e))
        typer.echo(f"[FAIL] Cannot open database: {e}", err=True)
        raise typer.Exit(code=1) from e

    try:
        ingestor = create_ingestor("injuries")

        if ingestor is None:
            typer.echo("[FAIL] Injury ingestor not found", err=True)
            raise typer.Exit(code=1)

        if team:
            entity_id = f"team:{team}"
            typer.echo(f"Ingesting injuries for {team} from {source}...")
        else:
            entity_id = "all"
            typer.echo(f"Ingesting all injuries from {source}...")

        result = ingestor.ingest(entity_id, conn, source=source)

        if result.get("status") == "SUCCESS":
            typer.echo(
                f"[OK] Successfully ingested {result.get('rows_affected', 0)} injury record(s)"
            )
        else:
            typer.echo(
                f"[FAIL] Ingestion failed: {result.get('error_message', 'Unknown error')}",
                err=True,
            )
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error("Injury ingestion failed", error=str(e))
        typer.echo(f"[FAIL] Injury ingestion failed: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()


@scrapers_app.command(name="ingest-contracts")
def ingest_contracts(
    team: str = typer.Option(
        None,
        "--team",
        "-t",
        help="Team identifier (name or abbreviation)",
    ),
    source: str = typer.Option(
        "realgm",
        "--source",
        help="Data source: realgm or spotrac",
    ),
) -> None:
    """
    Ingest player contract data from various sources.

    Fetches contract information including salary, contract type, and options.
    Use --team to filter by specific team.
    """
    from nba_vault.ingestion import create_ingestor

    try:
        conn = get_db_connection()
    except RuntimeError as e:
        logger.error("Cannot open database", error=str(e))
        typer.echo(f"[FAIL] Cannot open database: {e}", err=True)
        raise typer.Exit(code=1) from e

    try:
        ingestor = create_ingestor("contracts")

        if ingestor is None:
            typer.echo("[FAIL] Contract ingestor not found", err=True)
            raise typer.Exit(code=1)

        if team:
            entity_id = f"team:{team}"
            typer.echo(f"Ingesting contracts for {team} from {source}...")
        else:
            entity_id = "all"
            typer.echo(f"Ingesting all contracts from {source}...")

        result = ingestor.ingest(entity_id, conn, source=source)

        if result.get("status") == "SUCCESS":
            typer.echo(
                f"[OK] Successfully ingested {result.get('rows_affected', 0)} contract record(s)"
            )
        else:
            typer.echo(
                f"[FAIL] Ingestion failed: {result.get('error_message', 'Unknown error')}",
                err=True,
            )
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error("Contract ingestion failed", error=str(e))
        typer.echo(f"[FAIL] Contract ingestion failed: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()
