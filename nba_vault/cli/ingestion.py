"""Ingestion commands: ingest, ingest-players."""

import structlog
import typer

from nba_vault.schema.connection import get_db_connection
from nba_vault.utils.config import get_settings

ingestion_app = typer.Typer(help="Core data ingestion commands.")

logger = structlog.get_logger(__name__)


@ingestion_app.command()
def ingest(
    mode: str = typer.Option(
        "incremental",
        "--mode",
        "-m",
        help="Ingestion mode: 'incremental' or 'full'",
    ),
    workers: int = typer.Option(
        1,
        "--workers",
        "-w",
        help="Number of parallel workers (for full backfill)",
    ),
    start_season: int = typer.Option(
        None,
        "--start-season",
        help="Start season for backfill (e.g., 2024)",
    ),
    end_season: int = typer.Option(
        None,
        "--end-season",
        help="End season for backfill (e.g., 1946)",
    ),
) -> None:
    """
    Ingest NBA data from stats.nba.com and other sources.

    By default, performs incremental update for recent games.
    Use --mode full for complete historical backfill.
    """
    settings = get_settings()

    if mode == "full":
        start_season = start_season or settings.backfill_start_season
        end_season = end_season or settings.backfill_end_season
        workers = workers or settings.backfill_workers

    logger.info(
        "Starting ingestion",
        mode=mode,
        workers=workers,
        start_season=start_season,
        end_season=end_season,
    )

    try:
        if mode == "full":
            typer.echo(
                f"Starting full historical backfill from {start_season} to {end_season} "
                f"using {workers} worker(s)..."
            )
            # TODO: Implement full backfill logic
            typer.echo("[FAIL] Full backfill not yet implemented", err=True)
            raise typer.Exit(code=1)
        else:
            typer.echo("Starting incremental update...")
            # TODO: Implement incremental update logic
            typer.echo("[FAIL] Incremental update not yet implemented", err=True)
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error("Ingestion failed", error=str(e))
        typer.echo(f"[FAIL] Ingestion failed: {e}", err=True)
        raise typer.Exit(code=1) from e


@ingestion_app.command(name="ingest-players")
def ingest_players(
    season_end_year: int = typer.Option(
        None,
        "--season-end-year",
        "-s",
        help="Season end year (e.g., 2024 for 2023-24 season)",
    ),
    player_id: str = typer.Option(
        None,
        "--player-id",
        "-p",
        help="Basketball Reference player slug (e.g., 'jamesle01')",
    ),
) -> None:
    """
    Ingest player data from Basketball Reference.

    Fetches all players from a season by default.
    Use --player-id to fetch a specific player.
    """
    from nba_vault.ingestion import create_ingestor

    try:
        conn = get_db_connection()
    except RuntimeError as e:
        logger.error("Cannot open database", error=str(e))
        typer.echo(f"[FAIL] Cannot open database: {e}", err=True)
        raise typer.Exit(code=1) from e

    try:
        ingestor = create_ingestor("players")

        if ingestor is None:
            typer.echo("[FAIL] Players ingestor not found", err=True)
            raise typer.Exit(code=1)

        if player_id:
            typer.echo(f"Ingesting player: {player_id}...")
            entity_id = player_id
        else:
            if season_end_year is None:
                season_end_year = 2024
            typer.echo(f"Ingesting players from {season_end_year - 1}-{season_end_year} season...")
            entity_id = "season"

        result = ingestor.ingest(entity_id, conn, season_end_year=season_end_year)

        if result.get("status") == "SUCCESS":
            typer.echo(f"[OK] Successfully ingested {result.get('rows_affected', 0)} player(s)")
        else:
            typer.echo(
                f"[FAIL] Ingestion failed: {result.get('error_message', 'Unknown error')}",
                err=True,
            )
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error("Player ingestion failed", error=str(e))
        typer.echo(f"[FAIL] Player ingestion failed: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()
