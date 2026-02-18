#!/usr/bin/env pwsh
# Ingest all remaining data: lineups (all seasons), injuries, contracts, DuckDB export
# Run from project root: .\scripts\ingest_all_remaining.ps1

Set-Location $PSScriptRoot\..

$seasons = @("2013-14","2014-15","2015-16","2016-17","2017-18","2018-19","2019-20","2020-21","2021-22","2022-23","2023-24","2024-25")

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "STEP 1: Lineups ingestion (all seasons)" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

foreach ($season in $seasons) {
    Write-Host "`n=== Lineups: $season ===" -ForegroundColor Yellow
    uv run nba-vault advanced-stats ingest-lineups --scope league --season $season 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[WARN] Lineups $season exited with code $LASTEXITCODE" -ForegroundColor Red
    }
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "STEP 2: Injuries ingestion" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

Write-Host "`n=== Injuries: ESPN ===" -ForegroundColor Yellow
uv run nba-vault scrapers ingest-injuries --source espn 2>&1
Write-Host "`n=== Injuries: Rotowire ===" -ForegroundColor Yellow
uv run nba-vault scrapers ingest-injuries --source rotowire 2>&1

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "STEP 3: Contracts ingestion" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

Write-Host "`n=== Contracts: RealGM ===" -ForegroundColor Yellow
uv run nba-vault scrapers ingest-contracts --source realgm 2>&1
Write-Host "`n=== Contracts: Spotrac ===" -ForegroundColor Yellow
uv run nba-vault scrapers ingest-contracts --source spotrac 2>&1

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "STEP 4: Build DuckDB analytical database" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

uv run nba-vault export export --format duckdb 2>&1

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "ALL DONE!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
