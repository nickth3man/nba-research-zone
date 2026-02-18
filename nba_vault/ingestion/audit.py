"""Ingestion audit tracking."""

import sqlite3
from datetime import datetime
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class AuditLogger:
    """Track ingestion operations in the database."""

    def __init__(self, conn: sqlite3.Connection):
        """
        Initialize audit logger.

        Args:
            conn: SQLite database connection.
        """
        self.conn = conn
        self.logger = logger

    def log(
        self,
        entity_type: str,
        entity_id: str,
        source: str,
        status: str,
        row_count: int | None = None,
        error_message: str | None = None,
    ) -> None:
        """
        Log an ingestion operation.

        Args:
            entity_type: Type of entity (e.g., "player", "game").
            entity_id: Entity identifier.
            source: Data source (e.g., "nba_api", "basketball_reference").
            status: Status of the operation ("SUCCESS", "EMPTY", "FAILED", "SKIPPED").
            row_count: Number of rows affected.
            error_message: Error message if status is "FAILED".
        """
        ingest_ts = datetime.utcnow().isoformat()

        try:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO ingestion_audit
                (entity_type, entity_id, source, ingest_ts, status, row_count, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (entity_type, entity_id, source, ingest_ts, status, row_count, error_message),
            )
            try:
                self.conn.commit()
            except sqlite3.Error as commit_err:
                self.logger.error(
                    "Failed to commit audit log entry",
                    entity_type=entity_type,
                    entity_id=entity_id,
                    error=str(commit_err),
                )
        except sqlite3.Error as e:
            self.logger.error(
                "Failed to write audit log",
                entity_type=entity_type,
                entity_id=entity_id,
                error=str(e),
            )

    def get_status(self, entity_type: str, entity_id: str) -> dict[str, Any] | None:
        """
        Get the status of an entity's ingestion.

        Args:
            entity_type: Type of entity.
            entity_id: Entity identifier.

        Returns:
            Dictionary with status information or None if not found.
        """
        try:
            cursor = self.conn.execute(
                """
                SELECT entity_type, entity_id, source, ingest_ts, status, row_count, error_message
                FROM ingestion_audit
                WHERE entity_type = ? AND entity_id = ?
                ORDER BY ingest_ts DESC
                LIMIT 1
                """,
                (entity_type, entity_id),
            )
            row = cursor.fetchone()
        except sqlite3.Error as e:
            self.logger.error(
                "Failed to query audit status",
                entity_type=entity_type,
                entity_id=entity_id,
                error=str(e),
            )
            return None

        if row is None:
            return None

        return {
            "entity_type": row[0],
            "entity_id": row[1],
            "source": row[2],
            "ingest_ts": row[3],
            "status": row[4],
            "row_count": row[5],
            "error_message": row[6],
        }

    def get_failed_entities(self, entity_type: str | None = None) -> list[dict[str, Any]]:
        """
        Get list of failed ingestion operations.

        Args:
            entity_type: Filter by entity type. If None, returns all.

        Returns:
            List of dictionaries with failed entity information.
        """
        try:
            if entity_type:
                cursor = self.conn.execute(
                    """
                    SELECT entity_type, entity_id, source, ingest_ts, error_message
                    FROM ingestion_audit
                    WHERE entity_type = ? AND status = 'FAILED'
                    ORDER BY ingest_ts DESC
                    """,
                    (entity_type,),
                )
            else:
                cursor = self.conn.execute(
                    """
                    SELECT entity_type, entity_id, source, ingest_ts, error_message
                    FROM ingestion_audit
                    WHERE status = 'FAILED'
                    ORDER BY ingest_ts DESC
                    """
                )

            return [
                {
                    "entity_type": row[0],
                    "entity_id": row[1],
                    "source": row[2],
                    "ingest_ts": row[3],
                    "error_message": row[4],
                }
                for row in cursor.fetchall()
            ]
        except sqlite3.Error as e:
            self.logger.error(
                "Failed to query failed entities",
                entity_type=entity_type,
                error=str(e),
            )
            return []

    def get_stats(self) -> dict[str, Any]:
        """
        Get ingestion statistics.

        Returns:
            Dictionary with statistics.
        """
        try:
            cursor = self.conn.execute(
                """
                SELECT
                    entity_type,
                    status,
                    COUNT(*) as count,
                    SUM(row_count) as total_rows
                FROM ingestion_audit
                GROUP BY entity_type, status
                """
            )

            stats: dict[str, Any] = {}
            for row in cursor.fetchall():
                entity_type = row[0]
                status = row[1]
                count = row[2]
                total_rows = row[3] or 0

                if entity_type not in stats:
                    stats[entity_type] = {}

                stats[entity_type][status] = {"count": count, "total_rows": total_rows}

            return stats
        except sqlite3.Error as e:
            self.logger.error("Failed to query ingestion stats", error=str(e))
            return {}
