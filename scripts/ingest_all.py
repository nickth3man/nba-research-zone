"""Full historical NBA data ingestion orchestration script.

Implements the 6-stage pipeline from the Full Historical Ingestion Plan:

  Stage 0: Bulk open-source downloads (minutes, not days)
           - Seasons seed (required for RAPTOR FK)
           - ELO ratings (Neil-Paine/FiveThirtyEight, 1946-present)
           - RAPTOR player metrics (FiveThirtyEight, 1976-present)
           - PBP + shot charts (shufinskiy/nba_data, 1996-present, runs after Stage 2)

  Stage 1: Foundation API data (NBA.com, ~100 calls, ~15 min)
           - seasons (all 78), franchises, players (all years),
             draft (all years), draft_combine (2000+)

  Stage 2: Per-season API data (NBA.com, ~800 calls, ~2 hours)
           - game_schedule (reg + playoffs), lineups (1996+),
             team_advanced_stats, coaches (per team)
           - Shufinskiy PBP/shot charts run automatically after Stage 2

  Stage 3: Per-game API data — officials + box scores (NBA.com, ~65k calls, ~5.6 days)
           - game_officials, box_scores_traditional, box_scores_advanced,
             box_scores_hustle, team_other_stats

  Stage 4: Per-player API data (NBA.com, ~18k calls, ~38 hours)
           - player_bio, player_season_stats, awards, player_tracking (2013+)

  Stage 5: Scrapers + DuckDB export (~1 hour)
           - injuries (ESPN)
           - DuckDB analytical database export

Usage:
    # Full pipeline
    uv run python scripts/ingest_all.py

    # Run specific stages only
    uv run python scripts/ingest_all.py --stages 0,1,2

    # Skip Stage 3 (per-game API) — fastest path to usable data
    uv run python scripts/ingest_all.py --stages 0,1,2,4,5

    # Season range filter (applies to per-season and per-game stages)
    uv run python scripts/ingest_all.py --start-season 2000 --end-season 2024

    # Dry run — print what would be done without executing
    uv run python scripts/ingest_all.py --dry-run

    # Write live progress to a JSON file (readable without polling)
    uv run python scripts/ingest_all.py --status-file logs/progress.json

    # Background on Windows (PowerShell)
    Start-Process -NoNewWindow uv -ArgumentList "run python scripts/ingest_all.py"

Resumability:
    Every entity is tracked in ingestion_audit. Re-running the script skips
    entities already marked SUCCESS. To re-ingest a specific entity, delete
    its row from ingestion_audit first.

Log files:
    All output is written to logs/nba_vault_YYYYMMDD_HHMMSS.log as UTF-8 JSON.
    The console also receives structured output via structlog ConsoleRenderer.
    Use --status-file to get a live-updating JSON summary.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Ensure nba_vault is importable when running as a script
sys.path.insert(0, str(Path(__file__).parent.parent))

# Force UTF-8 on stdout/stderr before any import that might log
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Configure logging BEFORE any nba_vault imports that call structlog.
# stdout is reconfigured to UTF-8 first, then setup_logging() wires structlog.
# The subsequent imports are intentionally after the setup_logging() call.
from nba_vault.utils.logging import setup_logging

setup_logging()

import structlog  # noqa: E402, I001
from nba_vault.ingestion import (  # noqa: E402
    AwardsIngestor,
    BoxScoreAdvancedIngestor,
    BoxScoreHustleIngestor,
    BoxScoreTraditionalIngestor,
    CoachIngestor,
    DraftCombineIngestor,
    DraftIngestor,
    EloIngestor,
    FranchiseIngestor,
    GameOfficialIngestor,
    GameScheduleIngestor,
    InjuryIngestor,
    LineupsIngestor,
    PlayerBioIngestor,
    PlayerSeasonStatsIngestor,
    PlayerTrackingIngestor,
    PlayersIngestor,
    RaptorIngestor,
    SeasonIngestor,
    ShufinskiyPBPIngestor,
    TeamAdvancedStatsIngestor,
    TeamOtherStatsIngestor,
)
from nba_vault.ingestion.pre_modern_box_scores import PreModernBoxScoreIngestor  # noqa: E402
from nba_vault.ingestion.seasons import _SEASON_SEED  # noqa: E402
from nba_vault.schema.connection import get_db_connection  # noqa: E402

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Season constants
# ---------------------------------------------------------------------------
_ALL_SEASON_IDS = [s[0] for s in _SEASON_SEED]
_TRACKING_SEASON_IDS = [s for s in _ALL_SEASON_IDS if s >= 2013]
_CURRENT_SEASON = max(_ALL_SEASON_IDS)


def _season_label(season_id: int) -> str:
    return f"{season_id}-{str(season_id + 1)[-2:]}"


# ---------------------------------------------------------------------------
# Live status file
# ---------------------------------------------------------------------------

_status_file: Path | None = None
_status: dict[str, Any] = {
    "started_at": None,
    "pid": None,
    "stages_requested": [],
    "current_stage": None,
    "current_task": None,
    "stages": {},
    "errors": [],
    "finished_at": None,
    "exit_code": None,
}


def _init_status(stages: list[int], status_file: Path | None) -> None:
    global _status_file  # noqa: PLW0603
    _status_file = status_file
    _status["started_at"] = datetime.now(UTC).isoformat()
    _status["pid"] = __import__("os").getpid()
    _status["stages_requested"] = stages
    for s in stages:
        _status["stages"][str(s)] = {"state": "pending", "rows": 0, "errors": 0, "tasks": {}}
    _flush_status()


def _flush_status() -> None:
    if _status_file is None:
        return
    try:
        tmp = _status_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(_status, indent=2, default=str), encoding="utf-8")
        tmp.replace(_status_file)
    except OSError as e:
        logger.warning("Failed to write status file", path=str(_status_file), error=str(e))


def _stage_start(stage: int) -> None:
    _status["current_stage"] = stage
    _status["stages"][str(stage)]["state"] = "running"
    _status["stages"][str(stage)]["started_at"] = datetime.now(UTC).isoformat()
    _flush_status()
    logger.info("Stage started", stage=stage)


def _stage_done(stage: int, rows: int, errors: int) -> None:
    _status["stages"][str(stage)]["state"] = "done"
    _status["stages"][str(stage)]["finished_at"] = datetime.now(UTC).isoformat()
    _status["stages"][str(stage)]["rows"] = rows
    _status["stages"][str(stage)]["errors"] = errors
    _flush_status()
    logger.info("Stage complete", stage=stage, rows=rows, errors=errors)


def _task_update(stage: int, task: str, state: str, rows: int = 0, error: str = "") -> None:
    _status["current_task"] = task
    _status["stages"][str(stage)]["tasks"][task] = {
        "state": state,
        "rows": rows,
        "error": error,
        "ts": datetime.now(UTC).isoformat(),
    }
    _flush_status()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _already_done(conn: Any, entity_type: str, entity_id: str) -> bool:
    try:
        row = conn.execute(
            "SELECT 1 FROM ingestion_audit "
            "WHERE entity_type=? AND entity_id=? AND status='SUCCESS' LIMIT 1",
            (entity_type, entity_id),
        ).fetchone()
        return row is not None
    except Exception:
        return False


def _run_ingestor(
    ingestor: Any,
    entity_id: str,
    conn: Any,
    stage: int = -1,
    dry_run: bool = False,
    **kwargs: Any,
) -> dict[str, Any]:
    """Run a single ingestor with full observability: structured logs, status file, timing."""
    label = f"{ingestor.entity_type}:{entity_id}"

    if dry_run:
        logger.info("dry_run_skip", label=label)
        _task_update(stage, label, "dry_run")
        return {"status": "DRY_RUN", "entity_id": entity_id, "rows_affected": 0}

    if _already_done(conn, ingestor.entity_type, entity_id):
        logger.debug("already_done_skip", label=label)
        _task_update(stage, label, "skipped")
        return {"status": "SKIPPED", "entity_id": entity_id, "rows_affected": 0}

    _task_update(stage, label, "running")
    t0 = time.monotonic()

    try:
        result = ingestor.ingest(entity_id, conn, **kwargs)
    except Exception as exc:
        elapsed = round(time.monotonic() - t0, 2)
        tb = traceback.format_exc()
        logger.error(
            "ingestor_exception",
            label=label,
            elapsed_s=elapsed,
            exc_type=type(exc).__name__,
            exc_msg=str(exc),
            traceback=tb,
        )
        _task_update(stage, label, "error", error=f"{type(exc).__name__}: {exc}")
        _status["errors"].append(
            {"label": label, "ts": datetime.now(UTC).isoformat(), "error": str(exc)}
        )
        _flush_status()
        return {"status": "FAILED", "entity_id": entity_id, "error_message": str(exc)}

    elapsed = round(time.monotonic() - t0, 2)
    status = result.get("status", "UNKNOWN")
    rows = result.get("rows_affected", 0)

    if status == "FAILED":
        err_msg = result.get("error_message", "unknown")
        logger.error(
            "ingest_failed",
            label=label,
            status=status,
            elapsed_s=elapsed,
            error=err_msg,
        )
        _task_update(stage, label, "failed", rows=rows, error=err_msg)
        _status["errors"].append(
            {"label": label, "ts": datetime.now(UTC).isoformat(), "error": err_msg}
        )
        _flush_status()
    else:
        logger.info(
            "ingest_ok",
            label=label,
            status=status,
            rows=rows,
            elapsed_s=elapsed,
        )
        _task_update(stage, label, "done", rows=rows)

    return result


def _get_all_game_ids(conn: Any, season_ids: list[int]) -> list[str]:
    if not season_ids:
        return []
    placeholders = ",".join("?" * len(season_ids))
    rows = conn.execute(
        f"SELECT game_id FROM game WHERE season_id IN ({placeholders}) ORDER BY game_date",  # noqa: S608
        season_ids,
    ).fetchall()
    return [r[0] for r in rows]


def _get_all_player_ids(conn: Any) -> list[int]:
    rows = conn.execute("SELECT player_id FROM player ORDER BY player_id").fetchall()
    return [r[0] for r in rows]


def _get_all_team_ids(conn: Any, season_ids: list[int]) -> list[tuple[int, int]]:
    if not season_ids:
        return []
    placeholders = ",".join("?" * len(season_ids))
    rows = conn.execute(
        f"SELECT team_id, season_id FROM team WHERE season_id IN ({placeholders})",  # noqa: S608
        season_ids,
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _game_season_id(conn: Any, game_id: str) -> int | None:
    try:
        row = conn.execute(
            "SELECT season_id FROM game WHERE game_id=? LIMIT 1", (game_id,)
        ).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def _db_counts(conn: Any) -> dict[str, int]:
    """Return row counts for key tables — logged at stage boundaries."""
    tables = [
        "season",
        "franchise",
        "team",
        "player",
        "game",
        "game_elo",
        "player_raptor",
        "play_by_play",
        "shot_chart",
        "player_game_log",
        "draft",
        "award",
    ]
    counts: dict[str, int] = {}
    for t in tables:
        try:
            row = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()  # noqa: S608
            counts[t] = row[0] if row else 0
        except Exception:
            counts[t] = -1
    return counts


# ---------------------------------------------------------------------------
# Stage implementations
# ---------------------------------------------------------------------------


def stage0_bulk_downloads(conn: Any, dry_run: bool) -> None:
    """Stage 0: Seasons seed + ELO + RAPTOR bulk downloads.

    Shufinskiy PBP/shot charts run separately via stage0_shufinskiy() AFTER
    Stage 2 has populated the game table (FK dependency).
    """
    _stage_start(0)
    rows = errors = 0

    logger.info("stage0_begin", task="seasons_seed")
    r = _run_ingestor(SeasonIngestor(), "all", conn, stage=0, dry_run=dry_run)
    rows += r.get("rows_affected", 0)
    if r.get("status") == "FAILED":
        errors += 1

    logger.info("stage0_begin", task="elo_ratings")
    r = _run_ingestor(EloIngestor(), "all", conn, stage=0, dry_run=dry_run)
    rows += r.get("rows_affected", 0)
    if r.get("status") == "FAILED":
        errors += 1

    logger.info("stage0_begin", task="raptor_ratings")
    r = _run_ingestor(RaptorIngestor(), "all", conn, stage=0, dry_run=dry_run)
    rows += r.get("rows_affected", 0)
    if r.get("status") == "FAILED":
        errors += 1

    logger.info("stage0_db_snapshot", **_db_counts(conn))
    _stage_done(0, rows, errors)


def stage0_shufinskiy(conn: Any, dry_run: bool) -> None:
    """Stage 0b: Shufinskiy PBP + shot charts (requires games from Stage 2)."""
    game_count = conn.execute("SELECT COUNT(*) FROM game").fetchone()[0]
    logger.info("stage0b_shufinskiy_begin", game_count=game_count)
    if game_count == 0:
        logger.warning("stage0b_skip_no_games", reason="game table empty; run Stage 2 first")
        return

    _task_update(0, "shufinskiy_pbp:all", "running")
    r = _run_ingestor(ShufinskiyPBPIngestor(), "all", conn, stage=0, dry_run=dry_run)
    logger.info("stage0b_shufinskiy_done", rows=r.get("rows_affected", 0))


def stage1_foundation(conn: Any, dry_run: bool) -> None:
    """Stage 1: Seasons (idempotent), franchises, players (all years), draft."""
    _stage_start(1)
    rows = errors = 0

    for ingestor, eid, kwargs in [
        (SeasonIngestor(), "all", {}),
        (FranchiseIngestor(), "all", {}),
        (DraftIngestor(), "all", {}),
    ]:
        r = _run_ingestor(ingestor, eid, conn, stage=1, dry_run=dry_run, **kwargs)
        rows += r.get("rows_affected", 0)
        if r.get("status") == "FAILED":
            errors += 1

    # Players: iterate all years; ingestor is idempotent (ON CONFLICT UPDATE)
    players_ing = PlayersIngestor()
    for year in range(1947, _CURRENT_SEASON + 2):
        if dry_run:
            logger.info("dry_run_skip", label=f"players:season_{year}")
            continue
        logger.info("players_year_begin", year=year)
        try:
            result = players_ing.ingest("season", conn, season_end_year=year)
            yr = result.get("rows_affected", 0)
            rows += yr
            logger.info("players_year_done", year=year, rows=yr)
            _task_update(1, f"players:season_{year}", "done", rows=yr)
        except Exception as exc:
            errors += 1
            logger.error(
                "players_year_error",
                year=year,
                exc_type=type(exc).__name__,
                exc_msg=str(exc),
                traceback=traceback.format_exc(),
            )
            _task_update(1, f"players:season_{year}", "error", error=str(exc))
            _status["errors"].append(
                {
                    "label": f"players:season_{year}",
                    "ts": datetime.now(UTC).isoformat(),
                    "error": str(exc),
                }
            )
            _flush_status()

    # Draft combine (2000+)
    combine_ing = DraftCombineIngestor()
    for year in range(2000, _CURRENT_SEASON + 2):
        r = _run_ingestor(combine_ing, str(year), conn, stage=1, dry_run=dry_run)
        rows += r.get("rows_affected", 0)
        if r.get("status") == "FAILED":
            errors += 1

    logger.info("stage1_db_snapshot", **_db_counts(conn))
    _stage_done(1, rows, errors)


def stage2_per_season(conn: Any, dry_run: bool, season_ids: list[int]) -> None:
    """Stage 2: Per-season data — schedule, lineups, team stats, coaches."""
    _stage_start(2)
    rows = errors = 0
    total = len(season_ids)

    schedule_ing = GameScheduleIngestor()
    lineups_ing = LineupsIngestor()
    team_adv_ing = TeamAdvancedStatsIngestor()
    coach_ing = CoachIngestor()

    for i, season_id in enumerate(season_ids):
        label = _season_label(season_id)
        logger.info("stage2_season_begin", season=label, progress=f"{i + 1}/{total}")

        for r in [
            _run_ingestor(schedule_ing, label, conn, stage=2, dry_run=dry_run),
            _run_ingestor(
                schedule_ing, label, conn, stage=2, dry_run=dry_run, season_type="Playoffs"
            ),
        ]:
            rows += r.get("rows_affected", 0)
            if r.get("status") == "FAILED":
                errors += 1

        if season_id >= 1996:
            r = _run_ingestor(lineups_ing, label, conn, stage=2, dry_run=dry_run)
            rows += r.get("rows_affected", 0)
            if r.get("status") == "FAILED":
                errors += 1

        r = _run_ingestor(team_adv_ing, label, conn, stage=2, dry_run=dry_run, scope="league")
        rows += r.get("rows_affected", 0)
        if r.get("status") == "FAILED":
            errors += 1

        for team_id, _ in _get_all_team_ids(conn, [season_id]):
            r = _run_ingestor(coach_ing, str(team_id), conn, stage=2, dry_run=dry_run, season=label)
            rows += r.get("rows_affected", 0)
            if r.get("status") == "FAILED":
                errors += 1

        logger.info("stage2_season_done", season=label, cumulative_rows=rows)

    logger.info("stage2_db_snapshot", **_db_counts(conn))
    _stage_done(2, rows, errors)


def stage3_per_game(conn: Any, dry_run: bool, season_ids: list[int]) -> None:
    """Stage 3: Per-game data — officials, box scores, team other stats."""
    _stage_start(3)
    rows = errors = 0

    officials_ing = GameOfficialIngestor()
    box_trad_ing = BoxScoreTraditionalIngestor()
    box_adv_ing = BoxScoreAdvancedIngestor()
    box_hustle_ing = BoxScoreHustleIngestor()
    team_other_ing = TeamOtherStatsIngestor()

    game_ids = _get_all_game_ids(conn, season_ids)
    total = len(game_ids)
    logger.info("stage3_begin", total_games=total)

    for i, game_id in enumerate(game_ids):
        if i % 500 == 0:
            logger.info(
                "stage3_progress",
                done=i,
                total=total,
                pct=round(100 * i / total, 1) if total else 0,
                errors_so_far=errors,
            )
            _flush_status()

        season_id = _game_season_id(conn, game_id)

        r = _run_ingestor(officials_ing, game_id, conn, stage=3, dry_run=dry_run)
        rows += r.get("rows_affected", 0)
        if r.get("status") == "FAILED":
            errors += 1

        if season_id and season_id >= 1996:
            for ing in [box_trad_ing, box_adv_ing, team_other_ing]:
                r = _run_ingestor(ing, game_id, conn, stage=3, dry_run=dry_run)
                rows += r.get("rows_affected", 0)
                if r.get("status") == "FAILED":
                    errors += 1

        if season_id and season_id >= 2015:
            r = _run_ingestor(box_hustle_ing, game_id, conn, stage=3, dry_run=dry_run)
            rows += r.get("rows_affected", 0)
            if r.get("status") == "FAILED":
                errors += 1

    logger.info("stage3_db_snapshot", **_db_counts(conn))
    _stage_done(3, rows, errors)


def stage4_per_player(conn: Any, dry_run: bool) -> None:
    """Stage 4: Per-player data — bio, season stats, awards, tracking."""
    _stage_start(4)
    rows = errors = 0

    bio_ing = PlayerBioIngestor()
    stats_ing = PlayerSeasonStatsIngestor()
    awards_ing = AwardsIngestor()
    tracking_ing = PlayerTrackingIngestor()

    player_ids = _get_all_player_ids(conn)
    total = len(player_ids)
    logger.info("stage4_begin", total_players=total)

    for i, player_id in enumerate(player_ids):
        if i % 200 == 0:
            logger.info(
                "stage4_progress",
                done=i,
                total=total,
                pct=round(100 * i / total, 1) if total else 0,
                errors_so_far=errors,
            )
            _flush_status()

        pid = str(player_id)

        for ing, kwargs in [
            (bio_ing, {}),
            (stats_ing, {"per_mode": "Totals"}),
            (stats_ing, {"per_mode": "PerGame"}),
            (awards_ing, {}),
        ]:
            r = _run_ingestor(ing, pid, conn, stage=4, dry_run=dry_run, **kwargs)
            rows += r.get("rows_affected", 0)
            if r.get("status") == "FAILED":
                errors += 1

        for season_id in _TRACKING_SEASON_IDS:
            r = _run_ingestor(
                tracking_ing,
                pid,
                conn,
                stage=4,
                dry_run=dry_run,
                season=_season_label(season_id),
            )
            rows += r.get("rows_affected", 0)
            if r.get("status") == "FAILED":
                errors += 1

    logger.info("stage4_db_snapshot", **_db_counts(conn))
    _stage_done(4, rows, errors)


def stage5_scrapers_export(conn: Any, dry_run: bool) -> None:
    """Stage 5: Injuries scraper + DuckDB export."""
    _stage_start(5)
    rows = errors = 0

    r = _run_ingestor(InjuryIngestor(), "espn", conn, stage=5, dry_run=dry_run, source="espn")
    rows += r.get("rows_affected", 0)
    if r.get("status") == "FAILED":
        errors += 1

    if not dry_run:
        logger.info("stage5_duckdb_export_begin")
        try:
            import importlib  # noqa: PLC0415

            duckdb_builder = importlib.import_module("nba_vault.export.duckdb_builder")
            duckdb_builder.build_duckdb()
            logger.info("stage5_duckdb_export_done")
        except ImportError as exc:
            logger.warning(
                "stage5_duckdb_export_unavailable",
                hint="run: uv run nba-vault export --format duckdb",
                error=str(exc),
            )
        except Exception as exc:
            errors += 1
            logger.error(
                "stage5_duckdb_export_error",
                exc_type=type(exc).__name__,
                exc_msg=str(exc),
                traceback=traceback.format_exc(),
            )
    else:
        logger.info("dry_run_skip", label="duckdb_export")

    _stage_done(5, rows, errors)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Full historical NBA data ingestion pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--stages",
        default="0,1,2,3,4,5",
        help="Comma-separated list of stages to run (default: 0,1,2,3,4,5)",
    )
    parser.add_argument(
        "--start-season",
        type=int,
        default=1946,
        help="First season start year to include (default: 1946)",
    )
    parser.add_argument(
        "--end-season",
        type=int,
        default=_CURRENT_SEASON,
        help=f"Last season start year to include (default: {_CURRENT_SEASON})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without executing any ingestion",
    )
    parser.add_argument(
        "--pre-modern-csv",
        default=None,
        help="Path to eoinamoore PlayerStatistics.csv for pre-1996 box scores (optional)",
    )
    parser.add_argument(
        "--status-file",
        default=None,
        help="Path to write live JSON progress (e.g. logs/progress.json)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    stages_to_run = sorted({int(s.strip()) for s in args.stages.split(",")})
    season_ids = [s for s in _ALL_SEASON_IDS if args.start_season <= s <= args.end_season]
    status_file = Path(args.status_file) if args.status_file else None

    _init_status(stages_to_run, status_file)

    logger.info(
        "pipeline_start",
        stages=stages_to_run,
        season_range=f"{args.start_season}-{args.end_season}",
        season_count=len(season_ids),
        dry_run=args.dry_run,
        status_file=str(status_file) if status_file else None,
        log_file=str(
            __import__(
                "nba_vault.utils.logging", fromlist=["get_active_log_file"]
            ).get_active_log_file()
        ),
    )

    if args.dry_run:
        logger.info("dry_run_mode_active")

    conn = get_db_connection()
    exit_code = 0

    try:
        if 0 in stages_to_run:
            stage0_bulk_downloads(conn, dry_run=args.dry_run)

            if args.pre_modern_csv:
                r = _run_ingestor(
                    PreModernBoxScoreIngestor(),
                    args.pre_modern_csv,
                    conn,
                    stage=0,
                    dry_run=args.dry_run,
                )
                if r.get("status") == "FAILED":
                    logger.error("pre_modern_csv_failed", path=args.pre_modern_csv)

        if 1 in stages_to_run:
            stage1_foundation(conn, dry_run=args.dry_run)

        if 2 in stages_to_run:
            stage2_per_season(conn, dry_run=args.dry_run, season_ids=season_ids)
            if 0 in stages_to_run:
                stage0_shufinskiy(conn, dry_run=args.dry_run)

        if 3 in stages_to_run:
            stage3_per_game(conn, dry_run=args.dry_run, season_ids=season_ids)

        if 4 in stages_to_run:
            stage4_per_player(conn, dry_run=args.dry_run)

        if 5 in stages_to_run:
            stage5_scrapers_export(conn, dry_run=args.dry_run)

    except KeyboardInterrupt:
        logger.warning("pipeline_interrupted_by_user")
        exit_code = 130
    except Exception as exc:
        exit_code = 1
        logger.error(
            "pipeline_fatal_error",
            exc_type=type(exc).__name__,
            exc_msg=str(exc),
            traceback=traceback.format_exc(),
        )
    finally:
        conn.close()
        total_errors = len(_status["errors"])
        _status["finished_at"] = datetime.now(UTC).isoformat()
        _status["exit_code"] = exit_code
        _flush_status()
        logger.info(
            "pipeline_complete",
            exit_code=exit_code,
            total_errors=total_errors,
            db_snapshot=_db_counts(get_db_connection()),
        )

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
