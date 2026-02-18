# NBA Vault - Full Data Ingestion Script
# Runs the complete ingestion pipeline:
#   1. Initialize database
#   2. Full player backfill (1946-2024)
#   3. Advanced stats loop (2013-14 to 2024-25)
#   4. Injuries + Contracts (current season)
#   5. Build DuckDB export

$ErrorActionPreference = "Stop"
$StartTime = Get-Date

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "  $Message" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
}

function Write-OK {
    param([string]$Message)
    Write-Host "[OK] $Message" -ForegroundColor Green
}

function Write-FAIL {
    param([string]$Message)
    Write-Host "[FAIL] $Message" -ForegroundColor Red
}

function Write-INFO {
    param([string]$Message)
    Write-Host "  >> $Message" -ForegroundColor Yellow
}

# ─────────────────────────────────────────────────────────────────────────────
# Phase 1: Initialize Database
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "Phase 1: Initialize Database"
Write-INFO "Running migrations..."
uv run nba-vault admin init
if ($LASTEXITCODE -ne 0) {
    Write-FAIL "Database initialization failed"
    exit 1
}
Write-OK "Database initialized"

# ─────────────────────────────────────────────────────────────────────────────
# Phase 2: Full Player Backfill (1946-2024)
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "Phase 2: Player Backfill (1946-2024)"
Write-INFO "This ingests all players from Basketball Reference for every season..."

$StartSeason = 1946
$EndSeason   = 2025

for ($Year = $EndSeason; $Year -ge $StartSeason; $Year--) {
    Write-INFO "Ingesting players for season ending $Year ($($Year-1)-$Year)..."
    uv run nba-vault ingestion ingest-players --season-end-year $Year
    if ($LASTEXITCODE -ne 0) {
        Write-FAIL "Player ingestion failed for season $Year — continuing..."
    }
}
Write-OK "Player backfill complete"

# ─────────────────────────────────────────────────────────────────────────────
# Phase 3: Advanced Stats (2013-14 to 2024-25)
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "Phase 3: Advanced Stats (2013-14 to 2024-25)"

$AdvancedSeasons = @(
    "2013-14", "2014-15", "2015-16", "2016-17", "2017-18",
    "2018-19", "2019-20", "2020-21", "2021-22", "2022-23",
    "2023-24", "2024-25"
)

foreach ($Season in $AdvancedSeasons) {
    Write-INFO "[$Season] Ingesting lineups (league-wide)..."
    uv run nba-vault advanced-stats ingest-lineups --scope league --season $Season
    if ($LASTEXITCODE -ne 0) {
        Write-FAIL "Lineups ingestion failed for $Season — continuing..."
    }

    Write-INFO "[$Season] Ingesting team advanced stats (league-wide)..."
    uv run nba-vault advanced-stats ingest-team-advanced-stats --scope league --season $Season
    if ($LASTEXITCODE -ne 0) {
        Write-FAIL "Team advanced stats ingestion failed for $Season — continuing..."
    }
}
Write-OK "Advanced stats ingestion complete"

# ─────────────────────────────────────────────────────────────────────────────
# Phase 4: Web Scrapers (Current Season)
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "Phase 4: Injuries & Contracts (Current Season)"

Write-INFO "Ingesting injuries from ESPN..."
uv run nba-vault scrapers ingest-injuries --source espn
if ($LASTEXITCODE -ne 0) {
    Write-FAIL "ESPN injury ingestion failed — continuing..."
}

Write-INFO "Ingesting injuries from Rotowire..."
uv run nba-vault scrapers ingest-injuries --source rotowire
if ($LASTEXITCODE -ne 0) {
    Write-FAIL "Rotowire injury ingestion failed — continuing..."
}

Write-INFO "Ingesting contracts from RealGM..."
uv run nba-vault scrapers ingest-contracts --source realgm
if ($LASTEXITCODE -ne 0) {
    Write-FAIL "RealGM contract ingestion failed — continuing..."
}

Write-INFO "Ingesting contracts from Spotrac..."
uv run nba-vault scrapers ingest-contracts --source spotrac
if ($LASTEXITCODE -ne 0) {
    Write-FAIL "Spotrac contract ingestion failed — continuing..."
}

Write-OK "Scrapers complete"

# ─────────────────────────────────────────────────────────────────────────────
# Phase 5: Build DuckDB Analytical Database
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "Phase 5: Build DuckDB Analytical Database"
Write-INFO "Attaching SQLite and creating 11 analytical views..."
uv run nba-vault export export --format duckdb
if ($LASTEXITCODE -ne 0) {
    Write-FAIL "DuckDB export failed"
    exit 1
}
Write-OK "DuckDB database built"

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
$EndTime = Get-Date
$Duration = $EndTime - $StartTime

Write-Step "Ingestion Complete"
Write-Host ""
Write-Host "  Total time: $($Duration.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host ""

Write-INFO "Checking database status..."
uv run nba-vault admin status
