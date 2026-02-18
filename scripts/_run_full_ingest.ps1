# Full NBA Vault ingestion pipeline - generated runner
# ASCII-only to avoid encoding issues
Set-Location "$PSScriptRoot\.."
$ErrorActionPreference = "Continue"

# ------------------------------------------------------------------
# Transcript: capture everything (Write-Host + subprocess output) to
# a timestamped log file alongside the Python structured logs.
# ------------------------------------------------------------------
$LogsDir = Join-Path (Get-Location) "logs"
if (-not (Test-Path $LogsDir)) { New-Item -ItemType Directory -Path $LogsDir | Out-Null }
$TranscriptFile = Join-Path $LogsDir ("ingest_" + (Get-Date -Format "yyyyMMdd_HHmmss") + ".log")
Start-Transcript -Path $TranscriptFile -Append
Write-Host "Transcript -> $TranscriptFile"

# Phase 1: Initialize Database
Write-Host "PHASE 1: Initialize Database" -ForegroundColor Cyan

uv run nba-vault admin init 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL] Database initialization failed - aborting" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Database initialized" -ForegroundColor Green

# Phase 2: Player Backfill 1946-2025
Write-Host "PHASE 2: Player Backfill (1946-2025)" -ForegroundColor Cyan

for ($Year = 2025; $Year -ge 1946; $Year--) {
    Write-Host "--- Season end $Year ---" -ForegroundColor Yellow
    uv run nba-vault ingestion ingest-players --season-end-year $Year 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[WARN] ingest-players failed for $Year - continuing" -ForegroundColor Red
    }
}
Write-Host "[OK] Player backfill complete" -ForegroundColor Green

# Phase 3: Advanced Stats 2013-14 to 2024-25
Write-Host "PHASE 3: Advanced Stats (2013-14 to 2024-25)" -ForegroundColor Cyan

$AdvancedSeasons = @("2013-14","2014-15","2015-16","2016-17","2017-18","2018-19","2019-20","2020-21","2021-22","2022-23","2023-24","2024-25")

foreach ($Season in $AdvancedSeasons) {
    Write-Host "[$Season] lineups (league-wide)..." -ForegroundColor Yellow
    uv run nba-vault advanced-stats ingest-lineups --scope league --season $Season 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[WARN] lineups failed for $Season - continuing" -ForegroundColor Red
    }

    Write-Host "[$Season] team advanced stats (league-wide)..." -ForegroundColor Yellow
    uv run nba-vault advanced-stats ingest-team-advanced-stats --scope league --season $Season 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[WARN] team-advanced-stats failed for $Season - continuing" -ForegroundColor Red
    }
}
Write-Host "[OK] Advanced stats complete" -ForegroundColor Green

# Phase 3b: Reference / Seed data
Write-Host "PHASE 3b: Reference Data (franchises, draft, combine)" -ForegroundColor Cyan

Write-Host "Ingesting franchises..." -ForegroundColor Yellow
uv run nba-vault game-data ingest-franchises 2>&1

Write-Host "Ingesting full draft history..." -ForegroundColor Yellow
uv run nba-vault game-data ingest-draft 2>&1

for ($CombineYear = 2000; $CombineYear -le 2025; $CombineYear++) {
    Write-Host "Draft combine $CombineYear..." -ForegroundColor Yellow
    uv run nba-vault game-data ingest-draft-combine --year $CombineYear 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[WARN] combine $CombineYear failed - continuing" -ForegroundColor Red
    }
}
Write-Host "[OK] Reference data complete" -ForegroundColor Green

# Phase 4: Web Scrapers
Write-Host "PHASE 4: Injuries and Contracts" -ForegroundColor Cyan

Write-Host "Injuries - ESPN..." -ForegroundColor Yellow
uv run nba-vault scrapers ingest-injuries --source espn 2>&1

Write-Host "Injuries - Rotowire..." -ForegroundColor Yellow
uv run nba-vault scrapers ingest-injuries --source rotowire 2>&1

Write-Host "Contracts - RealGM (stub, expected NotImplementedError)..." -ForegroundColor Yellow
uv run nba-vault scrapers ingest-contracts --source realgm 2>&1

Write-Host "[OK] Scrapers complete" -ForegroundColor Green

# Phase 5: DuckDB Export
Write-Host "PHASE 5: Build DuckDB Analytical Database" -ForegroundColor Cyan

uv run nba-vault export export --format duckdb 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL] DuckDB export failed" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] DuckDB database built" -ForegroundColor Green

# Final Status
Write-Host "FINAL STATUS" -ForegroundColor Cyan
uv run nba-vault admin status 2>&1

Write-Host ""
Write-Host "Pipeline complete. Transcript saved -> $TranscriptFile" -ForegroundColor Cyan
Stop-Transcript
