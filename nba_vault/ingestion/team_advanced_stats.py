"""Advanced team stats ingestor.

This ingestor fetches advanced team statistics from NBA.com Stats API,
including offensive/defensive ratings, pace, four factors, etc.
"""

from typing import Any

import pydantic
import structlog

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.nba_stats_client import NBAStatsClient
from nba_vault.ingestion.registry import register_ingestor
from nba_vault.models.advanced_stats import TeamSeasonAdvancedCreate

logger = structlog.get_logger(__name__)


@register_ingestor
class TeamAdvancedStatsIngestor(BaseIngestor):
    """
    Ingestor for advanced team stats from NBA.com Stats API.

    Supports fetching advanced stats for specific teams or the entire league.
    Advanced stats include:
    - Offensive and defensive ratings
    - Net rating
    - Pace factor
    - Effective field goal percentage
    - Turnover percentage
    - Offensive rebounding percentage
    - Free throw rate
    - Three-point rate
    - True shooting percentage
    """

    entity_type = "team_advanced_stats"

    def __init__(self, cache=None, rate_limiter=None):
        """
        Initialize TeamAdvancedStatsIngestor.

        Args:
            cache: Content cache for API responses.
            rate_limiter: Rate limiter for API requests.
        """
        super().__init__(cache, rate_limiter)
        self.nba_client = NBAStatsClient(cache, rate_limiter)

    def fetch(
        self,
        entity_id: str,
        season: str,
        season_type: str = "Regular Season",
        measure_type: str = "Advanced",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Fetch advanced team stats from NBA.com Stats API.

        Args:
            entity_id: Team ID (as string) or "league" for all teams.
            season: Season in format "YYYY-YY" (e.g., "2023-24").
            season_type: "Regular Season", "Playoffs", or "Pre Season".
            measure_type: "Base", "Advanced", "Four Factors", etc.
            **kwargs: Additional parameters for the API request.

        Returns:
            Dictionary with advanced stats data.

        Raises:
            Exception: If fetch fails after retries.
        """
        if entity_id == "league":
            # Fetch all teams
            self.logger.info("Fetching advanced stats for all teams", season=season)

            data = self.nba_client.get_team_advanced_stats(
                season=season,
                season_type=season_type,
                measure_type=measure_type,
                **kwargs,
            )

            return {
                "scope": "league",
                "season": season,
                "season_type": season_type,
                "measure_type": measure_type,
                "data": data,
            }

        else:
            # Fetch specific team
            team_id = int(entity_id)
            self.logger.info("Fetching advanced stats for team", team_id=team_id, season=season)

            data = self.nba_client.get_team_advanced_stats(
                season=season,
                season_type=season_type,
                measure_type=measure_type,
                **kwargs,
            )

            return {
                "scope": "team",
                "team_id": team_id,
                "season": season,
                "season_type": season_type,
                "measure_type": measure_type,
                "data": data,
            }

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        """
        Validate raw advanced stats data using Pydantic models.

        Args:
            raw: Raw data dictionary from NBA.com API.

        Returns:
            List of validated TeamSeasonAdvancedCreate models.

        Raises:
            pydantic.ValidationError: If validation fails.
        """
        data = raw.get("data", {})
        scope = raw.get("scope", "team")
        season = raw.get("season")
        team_id = raw.get("team_id")

        validated_records = []

        # Extract season_id from season string
        season_year = int(season.split("-")[0])
        season_id = season_year

        # Process advanced stats data
        for _, dataset_data in data.items():
            if not isinstance(dataset_data, dict):
                continue

            data_rows = dataset_data.get("data", [])
            headers = dataset_data.get("headers", [])

            for row in data_rows:
                try:
                    # Map row data to field names using headers
                    row_dict = dict(zip(headers, row, strict=False)) if headers else {}

                    # Get team ID from row or use the team_id from request
                    row_team_id = row_dict.get("TEAM_ID")
                    if not row_team_id and team_id:
                        row_team_id = team_id
                    elif not row_team_id:
                        # Skip if we can't determine team ID
                        continue

                    # If we're looking for a specific team and this isn't it, skip
                    if scope == "team" and team_id and self._safe_int(row_team_id) != team_id:
                        continue

                    # Map advanced stats fields
                    # NBA.com field names may vary, so we need to handle variations
                    advanced_stats_data = {
                        "team_id": self._safe_int(row_team_id),
                        "season_id": season_id,
                        "off_rating": self._safe_float(
                            row_dict.get("OFF_RATING", row_dict.get("ORTG"))
                        ),
                        "def_rating": self._safe_float(
                            row_dict.get("DEF_RATING", row_dict.get("DRTG"))
                        ),
                        "net_rating": self._safe_float(
                            row_dict.get("NET_RATING", row_dict.get("NETRTG"))
                        ),
                        "pace": self._safe_float(row_dict.get("PACE")),
                        "effective_fg_pct": self._safe_float(
                            row_dict.get("EFG_pct", row_dict.get("EFG_PCT"))
                        ),
                        "turnover_pct": self._safe_float(
                            row_dict.get("TM_TOV_pct", row_dict.get("TOV_PCT"))
                        ),
                        "offensive_rebound_pct": self._safe_float(
                            row_dict.get("OREB_pct", row_dict.get("OREB_PCT"))
                        ),
                        "free_throw_rate": self._safe_float(
                            row_dict.get("FTA_RATE", row_dict.get("FT_RATE"))
                        ),
                        "three_point_rate": self._safe_float(
                            row_dict.get("FG3A_RATE", row_dict.get("THREE_POINT_RATE"))
                        ),
                        "true_shooting_pct": self._safe_float(
                            row_dict.get("TS_pct", row_dict.get("TS_PCT"))
                        ),
                    }

                    # Only add if we have a team_id
                    if advanced_stats_data["team_id"]:
                        validated_record = TeamSeasonAdvancedCreate(**advanced_stats_data)
                        validated_records.append(validated_record)

                except pydantic.ValidationError as e:
                    self.logger.error(
                        "Advanced stats validation failed",
                        row_data=row_dict,
                        errors=str(e),
                    )
                    raise

        self.logger.info("Validated advanced stats records", count=len(validated_records))
        return validated_records

    def upsert(self, model: list[pydantic.BaseModel], conn) -> int:
        """
        Insert or update validated advanced stats data in database.

        Args:
            model: List of validated TeamSeasonAdvancedCreate models.
            conn: SQLite database connection.

        Returns:
            Number of rows affected.
        """
        rows_affected = 0

        for stats_record in model:
            if not isinstance(stats_record, TeamSeasonAdvancedCreate):
                continue

            # Check if record exists
            cursor = conn.execute(
                """
                SELECT COUNT(*) FROM team_season_advanced
                WHERE team_id = ? AND season_id = ?
                """,
                (stats_record.team_id, stats_record.season_id),
            )
            exists = cursor.fetchone()[0] > 0

            if exists:
                # Update existing record
                self._update_advanced_stats(stats_record, conn)
                rows_affected += 1
            else:
                # Insert new record
                self._insert_advanced_stats(stats_record, conn)
                rows_affected += 1

            # Log to ingestion_audit
            conn.execute(
                """
                INSERT INTO ingestion_audit
                (entity_type, entity_id, status, source, metadata, ingested_at)
                VALUES (?, ?, 'SUCCESS', 'nba_stats_api', ?, datetime('now'))
                """,
                (
                    self.entity_type,
                    f"{stats_record.team_id}_{stats_record.season_id}",
                    f"season: {stats_record.season_id}, team: {stats_record.team_id}",
                ),
            )

        return rows_affected

    def _insert_advanced_stats(self, stats: TeamSeasonAdvancedCreate, conn) -> None:
        """
        Insert a new advanced stats record into the database.

        Args:
            stats: TeamSeasonAdvancedCreate model.
            conn: SQLite database connection.
        """
        conn.execute(
            """
            INSERT INTO team_season_advanced (
                team_id, season_id, off_rating, def_rating, net_rating,
                pace, effective_fg_pct, turnover_pct, offensive_rebound_pct,
                free_throw_rate, three_point_rate, true_shooting_pct
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stats.team_id,
                stats.season_id,
                stats.off_rating,
                stats.def_rating,
                stats.net_rating,
                stats.pace,
                stats.effective_fg_pct,
                stats.turnover_pct,
                stats.offensive_rebound_pct,
                stats.free_throw_rate,
                stats.three_point_rate,
                stats.true_shooting_pct,
            ),
        )

    def _update_advanced_stats(self, stats: TeamSeasonAdvancedCreate, conn) -> None:
        """
        Update an existing advanced stats record in the database.

        Args:
            stats: TeamSeasonAdvancedCreate model.
            conn: SQLite database connection.
        """
        conn.execute(
            """
            UPDATE team_season_advanced SET
                off_rating = ?, def_rating = ?, net_rating = ?,
                pace = ?, effective_fg_pct = ?, turnover_pct = ?,
                offensive_rebound_pct = ?, free_throw_rate = ?,
                three_point_rate = ?, true_shooting_pct = ?
            WHERE team_id = ? AND season_id = ?
            """,
            (
                stats.off_rating,
                stats.def_rating,
                stats.net_rating,
                stats.pace,
                stats.effective_fg_pct,
                stats.turnover_pct,
                stats.offensive_rebound_pct,
                stats.free_throw_rate,
                stats.three_point_rate,
                stats.true_shooting_pct,
                stats.team_id,
                stats.season_id,
            ),
        )

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        """Safely convert value to int, returning None for empty/invalid values."""
        if value is None or value in {"", "-"}:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        """Safely convert value to float, returning None for empty/invalid values."""
        if value is None or value in {"", "-"}:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
