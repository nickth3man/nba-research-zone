"""Export commands: export."""

import csv
import json
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
import typer

from nba_vault.duckdb.builder import build_duckdb_database
from nba_vault.schema.connection import get_db_connection

export_app = typer.Typer(help="Database export commands.")

logger = structlog.get_logger(__name__)

# Internal tables to exclude from exports
INTERNAL_TABLES = {
    "_yoyo_log",
    "_yoyo_migration",
    "_yoyo_version",
    "sqlite_sequence",
    "yoyo_lock",
}

# SQL identifier validation (prevent injection)
VALID_TABLE_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


@export_app.command()
def export(
    format: str = typer.Option(
        "duckdb",
        "--format",
        "-f",
        help="Export format: duckdb, csv, json, or parquet",
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

    Formats:
    - duckdb: Build DuckDB analytical database with pre-built views
    - csv: Export each table to a separate CSV file
    - json: Full database dump as JSON
    - parquet: Export each table to a separate Parquet file
    """
    logger.info(
        "Starting export",
        format=format,
        output_dir=str(output_dir),
        entities=entities or ["all"],
    )

    try:
        if format == "duckdb":
            _export_duckdb()
        elif format == "csv":
            _export_csv(output_dir, entities)
        elif format == "json":
            _export_json(output_dir, entities)
        elif format == "parquet":
            _export_parquet(output_dir, entities)
        else:
            typer.echo(f"[FAIL] Unknown export format '{format}'", err=True)
            raise typer.Exit(code=1)

    except Exception as e:
        logger.exception("Export failed", error=str(e))
        typer.echo(f"[FAIL] Export failed: {e}", err=True)
        raise typer.Exit(code=1) from e


def _get_tables(entities: list[str] | None, conn) -> list[str]:
    """Get list of tables to export, filtering by entities if specified."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    all_tables = [row[0] for row in cursor.fetchall()]

    # Filter out internal tables
    tables = [t for t in all_tables if t not in INTERNAL_TABLES]

    # Filter by entities if specified
    if entities:
        entities_lower = [e.lower() for e in entities]
        tables = [t for t in tables if t.lower() in entities_lower]
        if not tables:
            available = ", ".join(sorted(t for t in all_tables if t not in INTERNAL_TABLES))
            raise ValueError(f"No matching tables found. Available: {available}")

    return tables


def _validate_table_name(table: str) -> None:
    """Validate table name to prevent SQL injection."""
    if not VALID_TABLE_NAME.match(table):
        raise ValueError(f"Invalid table name: {table}")


def _export_duckdb() -> None:
    """Export to DuckDB format."""
    typer.echo("Building DuckDB analytical database...")
    build_duckdb_database()
    typer.echo("[OK] DuckDB database built successfully")


def _export_csv(output_dir: Path, entities: list[str] | None) -> None:
    """Export database to CSV format (one file per table)."""
    typer.echo(f"Exporting to CSV format in {output_dir}/...")

    conn = get_db_connection()
    try:
        tables = _get_tables(entities, conn)
        csv_dir = output_dir / "csv"
        csv_dir.mkdir(parents=True, exist_ok=True)

        total_rows = 0
        for i, table in enumerate(tables, 1):
            _validate_table_name(table)
            cursor = conn.cursor()
            cursor.execute(f'SELECT * FROM "{table}"')  # noqa: S608
            rows = cursor.fetchall()

            if rows:
                columns = [desc[0] for desc in cursor.description]
                csv_path = csv_dir / f"{table}.csv"

                with csv_path.open("w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(columns)
                    writer.writerows(rows)

                file_size = csv_path.stat().st_size
                total_rows += len(rows)

                logger.info(
                    "Exported table",
                    table=table,
                    rows=len(rows),
                    file_size_bytes=file_size,
                )

                typer.echo(f"  [{i}/{len(tables)}] {table}: {len(rows):,} rows → {csv_path}")

        logger.info(
            "CSV export complete",
            output_dir=str(csv_dir),
            tables_exported=len(tables),
            total_rows=total_rows,
        )
        typer.echo(f"[OK] Exported {len(tables)} tables ({total_rows:,} rows) to {csv_dir}")

    finally:
        conn.close()


def _export_json(output_dir: Path, entities: list[str] | None) -> None:
    """Export database to JSON format (single file with all tables)."""
    typer.echo(f"Exporting to JSON format in {output_dir}/...")

    conn = get_db_connection()
    try:
        tables = _get_tables(entities, conn)
        output_dir.mkdir(parents=True, exist_ok=True)
        json_path = output_dir / "nba_full.json"

        data = {}
        total_rows = 0

        for i, table in enumerate(tables, 1):
            _validate_table_name(table)
            cursor = conn.cursor()
            cursor.execute(f'SELECT * FROM "{table}"')  # noqa: S608
            rows = cursor.fetchall()

            if rows:
                columns = [desc[0] for desc in cursor.description]
                data[table] = [dict(zip(columns, row, strict=True)) for row in rows]
                total_rows += len(rows)

                logger.info("Exported table", table=table, rows=len(rows))
                typer.echo(f"  [{i}/{len(tables)}] {table}: {len(rows):,} rows")

        # Write JSON file
        with json_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        file_size = json_path.stat().st_size

        logger.info(
            "JSON export complete",
            output_path=str(json_path),
            tables_exported=len(tables),
            total_rows=total_rows,
            file_size_bytes=file_size,
        )
        typer.echo(f"[OK] Exported {len(tables)} tables ({total_rows:,} rows) to {json_path}")
        typer.echo(f"  File size: {file_size:,} bytes")

    finally:
        conn.close()


def _export_parquet(output_dir: Path, entities: list[str] | None) -> None:
    """Export database to Parquet format (one file per table)."""
    typer.echo(f"Exporting to Parquet format in {output_dir}/...")

    conn = get_db_connection()
    try:
        tables = _get_tables(entities, conn)
        parquet_dir = output_dir / "parquet"
        parquet_dir.mkdir(parents=True, exist_ok=True)

        total_rows = 0
        for i, table in enumerate(tables, 1):
            _validate_table_name(table)
            cursor = conn.cursor()
            cursor.execute(f'SELECT * FROM "{table}"')  # noqa: S608
            rows = cursor.fetchall()

            if rows:
                columns = [desc[0] for desc in cursor.description]

                # Convert rows to list of dicts for PyArrow
                data = [dict(zip(columns, row, strict=True)) for row in rows]

                # Create PyArrow table
                arrow_table = pa.table(data)

                # Write to Parquet
                parquet_path = parquet_dir / f"{table}.parquet"
                pq.write_table(arrow_table, parquet_path)

                file_size = parquet_path.stat().st_size
                total_rows += len(rows)

                logger.info(
                    "Exported table",
                    table=table,
                    rows=len(rows),
                    file_size_bytes=file_size,
                )

                typer.echo(f"  [{i}/{len(tables)}] {table}: {len(rows):,} rows → {parquet_path}")

        logger.info(
            "Parquet export complete",
            output_dir=str(parquet_dir),
            tables_exported=len(tables),
            total_rows=total_rows,
        )
        typer.echo(f"[OK] Exported {len(tables)} tables ({total_rows:,} rows) to {parquet_dir}")

    finally:
        conn.close()
