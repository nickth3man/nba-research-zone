"""Player data ingestor."""

import sqlite3
from typing import Any, cast

import pydantic
import structlog

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.basketball_reference import BasketballReferenceClient
from nba_vault.ingestion.registry import register_ingestor
from nba_vault.models.players import BasketballReferencePlayer, PlayerCreate

logger = structlog.get_logger(__name__)


@register_ingestor
class PlayersIngestor(BaseIngestor):
    """
    Ingestor for player data from Basketball Reference.

    Supports fetching single players or entire season rosters.
    """

    entity_type = "players"

    def __init__(self, cache=None, rate_limiter=None):
        """
        Initialize PlayersIngestor.

        Args:
            cache: Content cache for API responses.
            rate_limiter: Rate limiter for API requests.
        """
        super().__init__(cache, rate_limiter)
        self.basketball_reference_client = BasketballReferenceClient(cache, rate_limiter)

    def fetch(
        self, entity_id: str, season_end_year: int | None = None, **kwargs: Any
    ) -> dict[str, Any]:
        """
        Fetch player data from Basketball Reference.

        Args:
            entity_id: Player slug (e.g., "jamesle01") or "season" for all players.
            season_end_year: Season end year (e.g., 2024 for 2023-24 season).
            **kwargs: Additional parameters (ignored).

        Returns:
            Dictionary with player data or list of players.

        Raises:
            Exception: If fetch fails after retries.
        """
        if entity_id in {"season", "all"}:
            # Fetch all players for a season
            players_data = self.basketball_reference_client.get_players(season_end_year)
            return {"players": players_data, "season_end_year": season_end_year}
        else:
            # Fetch single player (future enhancement)
            self.logger.info("Fetching single player", player_id=entity_id)
            # For now, fetch season and filter
            all_players = self.basketball_reference_client.get_players(season_end_year)
            for player in all_players:
                if player.get("slug") == entity_id:
                    return {"players": [player], "season_end_year": season_end_year}

            raise ValueError(f"Player {entity_id} not found in season {season_end_year}")

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        """
        Validate raw player data using Pydantic models.

        Args:
            raw: Raw data dictionary with 'players' key containing list of player dicts.

        Returns:
            List of validated BasketballReferencePlayer models.

        Raises:
            pydantic.ValidationError: If validation fails.
        """
        players_data = raw.get("players", [])

        validated_players = []
        for player_data in players_data:
            try:
                # Validate individual player data
                validated_player = BasketballReferencePlayer(**player_data)
                validated_players.append(validated_player)
            except pydantic.ValidationError as e:
                self.logger.error(
                    "Player validation failed",
                    player_data=player_data,
                    errors=str(e),
                )
                raise

        self.logger.info("Validated players", count=len(validated_players))
        return validated_players

    def upsert(self, model: list[pydantic.BaseModel], conn) -> int:
        """
        Insert or update validated player data in database.

        Args:
            model: List of validated BasketballReferencePlayer models.
            conn: SQLite database connection.

        Returns:
            Number of rows affected.
        """
        rows_affected = 0

        try:
            conn.execute("BEGIN")

            for validated_player in model:
                # Convert BasketballReferencePlayer to PlayerCreate
                br_player = cast("BasketballReferencePlayer", validated_player)
                player_create = PlayerCreate.from_basketball_reference(br_player)

                # Check if player exists by bbref_id
                cursor = conn.execute(
                    "SELECT player_id FROM player WHERE bbref_id = ?", (player_create.bbref_id,)
                )
                existing = cursor.fetchone()

                if existing:
                    # Update existing player
                    player_create.player_id = existing[0]
                    self._update_player(player_create, conn)
                    rows_affected += 1
                else:
                    # Insert new player
                    # Use nba_person_id as player_id if available, else auto-increment
                    if player_create.player_id is None:
                        cursor = conn.execute("SELECT COALESCE(MAX(player_id), 0) FROM player")
                        max_id = cursor.fetchone()[0]
                        player_create.player_id = max_id + 1

                    self._insert_player(player_create, conn)
                    rows_affected += 1

                # Log to ingestion_audit
                conn.execute(
                    """
                    INSERT INTO ingestion_audit
                    (entity_type, entity_id, status, source, ingest_ts, row_count)
                    VALUES (?, ?, 'SUCCESS', 'nba_api', datetime('now'), 1)
                    """,
                    (
                        self.entity_type,
                        player_create.bbref_id,
                    ),
                )

            conn.execute("COMMIT")

        except sqlite3.IntegrityError as exc:
            conn.execute("ROLLBACK")
            self.logger.warning(
                "Integrity error during player upsert",
                rows_before_error=rows_affected,
                error=str(exc),
            )
            raise
        except sqlite3.OperationalError as exc:
            conn.execute("ROLLBACK")
            self.logger.error(
                "Operational error during player upsert",
                rows_before_error=rows_affected,
                error=str(exc),
            )
            raise

        return rows_affected

    def _insert_player(self, player: PlayerCreate, conn) -> None:
        """
        Insert a new player into the database.

        Args:
            player: PlayerCreate model.
            conn: SQLite database connection.
        """
        conn.execute(
            """
            INSERT INTO player (
                player_id, first_name, last_name, full_name, display_name,
                birthdate, birthplace_city, birthplace_state, birthplace_country,
                height_inches, weight_lbs, position, primary_position,
                jersey_number, college, country, draft_year, draft_round,
                draft_number, is_active, from_year, to_year, bbref_id,
                data_availability_flags
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                player.player_id,
                player.first_name,
                player.last_name,
                player.full_name,
                player.display_name,
                player.birthdate,
                player.birthplace_city,
                player.birthplace_state,
                player.birthplace_country,
                player.height_inches,
                player.weight_lbs,
                player.position,
                player.primary_position,
                player.jersey_number,
                player.college,
                player.country,
                player.draft_year,
                player.draft_round,
                player.draft_number,
                1 if player.is_active else 0,  # Convert bool to int
                player.from_year,
                player.to_year,
                player.bbref_id,
                player.data_availability_flags,
            ),
        )

    def _update_player(self, player: PlayerCreate, conn) -> None:
        """
        Update an existing player in the database.

        Args:
            player: PlayerCreate model.
            conn: SQLite database connection.
        """
        conn.execute(
            """
            UPDATE player SET
                first_name = ?, last_name = ?, full_name = ?, display_name = ?,
                birthdate = ?, birthplace_city = ?, birthplace_state = ?, birthplace_country = ?,
                height_inches = ?, weight_lbs = ?, position = ?, primary_position = ?,
                jersey_number = ?, college = ?, country = ?, draft_year = ?,
                draft_round = ?, draft_number = ?, is_active = ?,
                from_year = ?, to_year = ?, bbref_id = ?,
                data_availability_flags = ?
            WHERE player_id = ?
            """,
            (
                player.first_name,
                player.last_name,
                player.full_name,
                player.display_name,
                player.birthdate,
                player.birthplace_city,
                player.birthplace_state,
                player.birthplace_country,
                player.height_inches,
                player.weight_lbs,
                player.position,
                player.primary_position,
                player.jersey_number,
                player.college,
                player.country,
                player.draft_year,
                player.draft_round,
                player.draft_number,
                1 if player.is_active else 0,
                player.from_year,
                player.to_year,
                player.bbref_id,
                player.data_availability_flags,
                player.player_id,
            ),
        )
