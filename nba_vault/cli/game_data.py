"""Game-data ingestion commands: seasons, franchises, schedule, officials,
box-scores (traditional / advanced / hustle), play-by-play, shot charts,
player bio, coaches, draft, draft-combine, awards, player-season-stats.
"""

import structlog
import typer

from nba_vault.schema.connection import get_db_connection

game_data_app = typer.Typer(help="Game and player data ingestion commands.")

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(entity_id: str, ingestor_type: str, label: str, **kwargs: object) -> None:  # type: ignore[misc]
    """Create an ingestor, run ingest(), and print a status line."""
    from nba_vault.ingestion import create_ingestor

    try:
        conn = get_db_connection()
    except RuntimeError as e:
        logger.error("Cannot open database", error=str(e))
        typer.echo(f"[FAIL] Cannot open database: {e}", err=True)
        raise typer.Exit(code=1) from e

    try:
        ingestor = create_ingestor(ingestor_type)
        if ingestor is None:
            typer.echo(f"[FAIL] Ingestor '{ingestor_type}' not found", err=True)
            raise typer.Exit(code=1)

        typer.echo(f"Ingesting {label}…")
        result = ingestor.ingest(entity_id, conn, **kwargs)

        if result.get("status") == "SUCCESS":
            typer.echo(f"[OK] {label} — {result.get('rows_affected', 0)} row(s) affected")
        else:
            typer.echo(
                f"[FAIL] {label}: {result.get('error_message', 'unknown error')}",
                err=True,
            )
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error(f"{label} ingestion failed", error=str(e))
        typer.echo(f"[FAIL] {label} failed: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Seasons / Franchises
# ---------------------------------------------------------------------------


@game_data_app.command(name="ingest-seasons")
def ingest_seasons(
    season: str = typer.Option(
        ...,
        "--season",
        "-s",
        help="Season in YYYY-YY format (e.g. 2023-24).",
    ),
) -> None:
    """Ingest season metadata (schedule frame, standings context)."""
    _run(season, "seasons", f"seasons {season}")


@game_data_app.command(name="ingest-franchises")
def ingest_franchises() -> None:
    """Ingest franchise / team history records from NBA.com."""
    _run("all", "franchises", "franchises")


# ---------------------------------------------------------------------------
# Games / Officials
# ---------------------------------------------------------------------------


@game_data_app.command(name="ingest-schedule")
def ingest_schedule(
    season: str = typer.Option(
        ...,
        "--season",
        "-s",
        help="Season in YYYY-YY format (e.g. 2023-24).",
    ),
    team_id: int = typer.Option(
        None,
        "--team-id",
        help="Filter to a single NBA.com team ID.",
    ),
) -> None:
    """Ingest game schedule for a season."""
    entity_id = str(team_id) if team_id else "all"
    _run(entity_id, "game_schedule", f"schedule {season}", season=season)


@game_data_app.command(name="ingest-officials")
def ingest_officials(
    game_id: str = typer.Option(
        ...,
        "--game-id",
        "-g",
        help="10-character NBA.com game ID (e.g. 0022300001).",
    ),
) -> None:
    """Ingest officials (referees) for a single game."""
    _run(game_id, "game_officials", f"officials for game {game_id}")


# ---------------------------------------------------------------------------
# Box scores
# ---------------------------------------------------------------------------


@game_data_app.command(name="ingest-box-scores")
def ingest_box_scores(
    game_id: str = typer.Option(
        ...,
        "--game-id",
        "-g",
        help="10-character NBA.com game ID.",
    ),
) -> None:
    """Ingest traditional (player + team) box score for a game."""
    _run(game_id, "box_scores_traditional", f"traditional box score {game_id}")


@game_data_app.command(name="ingest-box-scores-advanced")
def ingest_box_scores_advanced(
    game_id: str = typer.Option(
        ...,
        "--game-id",
        "-g",
        help="10-character NBA.com game ID.",
    ),
) -> None:
    """Ingest advanced box score for a game (TS%, USG%, etc.)."""
    _run(game_id, "box_scores_advanced", f"advanced box score {game_id}")


@game_data_app.command(name="ingest-box-scores-hustle")
def ingest_box_scores_hustle(
    game_id: str = typer.Option(
        ...,
        "--game-id",
        "-g",
        help="10-character NBA.com game ID.",
    ),
) -> None:
    """Ingest hustle-stats box score for a game (2015-16+)."""
    _run(game_id, "box_scores_hustle", f"hustle box score {game_id}")


# ---------------------------------------------------------------------------
# Play-by-play
# ---------------------------------------------------------------------------


@game_data_app.command(name="ingest-pbp")
def ingest_pbp(
    game_id: str = typer.Option(
        ...,
        "--game-id",
        "-g",
        help="10-character NBA.com game ID.",
    ),
) -> None:
    """Ingest play-by-play event log for a game."""
    _run(game_id, "play_by_play", f"play-by-play {game_id}")


# ---------------------------------------------------------------------------
# Shot charts
# ---------------------------------------------------------------------------


@game_data_app.command(name="ingest-shot-charts")
def ingest_shot_charts(
    player_id: int = typer.Option(
        None,
        "--player-id",
        help="NBA.com player ID.",
    ),
    game_id: str = typer.Option(
        None,
        "--game-id",
        "-g",
        help="10-character NBA.com game ID.",
    ),
    season: str = typer.Option(
        None,
        "--season",
        "-s",
        help="Season in YYYY-YY format (e.g. 2023-24).",
    ),
) -> None:
    """Ingest shot chart (FGA locations) for a player or game."""
    if player_id:
        entity_id = str(player_id)
        label = f"shot chart player {player_id}"
    elif game_id:
        entity_id = game_id
        label = f"shot chart game {game_id}"
    else:
        typer.echo("[FAIL] Provide --player-id or --game-id", err=True)
        raise typer.Exit(code=1)

    kwargs: dict[str, object] = {}
    if season:
        kwargs["season"] = season

    _run(entity_id, "shot_chart", label, **kwargs)


# ---------------------------------------------------------------------------
# Player bio / coaches
# ---------------------------------------------------------------------------


@game_data_app.command(name="ingest-player-bio")
def ingest_player_bio(
    player_id: int = typer.Option(
        ...,
        "--player-id",
        help="NBA.com player ID.",
    ),
) -> None:
    """Ingest biographical details for a player (height, weight, position, etc.)."""
    _run(str(player_id), "player_bio", f"player bio {player_id}")


@game_data_app.command(name="ingest-coaches")
def ingest_coaches(
    team_id: int = typer.Option(
        ...,
        "--team-id",
        help="NBA.com team ID.",
    ),
    season: str = typer.Option(
        ...,
        "--season",
        "-s",
        help="Season in YYYY-YY format (e.g. 2023-24).",
    ),
) -> None:
    """Ingest coaching staff for a team/season."""
    _run(str(team_id), "coaches", f"coaches team {team_id} {season}", season=season)


# ---------------------------------------------------------------------------
# Draft
# ---------------------------------------------------------------------------


@game_data_app.command(name="ingest-draft")
def ingest_draft(
    year: int = typer.Option(
        None,
        "--year",
        "-y",
        help="Draft year (e.g. 2024). Omit to load all years.",
    ),
) -> None:
    """Ingest draft history (all years or a specific year)."""
    entity_id = str(year) if year else "all"
    label = f"draft {year}" if year else "draft history (all years)"
    _run(entity_id, "draft", label)


@game_data_app.command(name="ingest-draft-combine")
def ingest_draft_combine(
    year: int = typer.Option(
        ...,
        "--year",
        "-y",
        help="Draft combine year (e.g. 2024). Available from 2000 onwards.",
    ),
) -> None:
    """Ingest draft combine measurements + drill times for a year."""
    _run(str(year), "draft_combine", f"draft combine {year}")


# ---------------------------------------------------------------------------
# Awards / season stats
# ---------------------------------------------------------------------------


@game_data_app.command(name="ingest-awards")
def ingest_awards(
    player_id: int = typer.Option(
        ...,
        "--player-id",
        help="NBA.com player ID.",
    ),
) -> None:
    """Ingest career awards (MVP, All-Star, All-NBA, etc.) for a player."""
    _run(str(player_id), "awards", f"awards player {player_id}")


@game_data_app.command(name="ingest-season-stats")
def ingest_season_stats(
    player_id: int = typer.Option(
        ...,
        "--player-id",
        help="NBA.com player ID.",
    ),
    per_mode: str = typer.Option(
        "Totals",
        "--per-mode",
        help="Per-mode: Totals, PerGame, Per36, Per100Possessions.",
    ),
) -> None:
    """Ingest per-season statistics for a player (career totals or averages)."""
    _run(
        str(player_id),
        "player_season_stats",
        f"season stats player {player_id} ({per_mode})",
        per_mode=per_mode,
    )
