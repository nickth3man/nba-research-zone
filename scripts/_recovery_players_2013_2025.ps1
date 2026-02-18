# Recovery: re-ingest player seasons 2013-2025 that failed in the initial run
# due to UNIQUE constraint bug (now fixed with INSERT OR REPLACE).
# Run this AFTER _run_full_ingest.ps1 completes.
Set-Location "$PSScriptRoot\.."
$ErrorActionPreference = "Continue"

Write-Host "RECOVERY: Re-ingesting player seasons 2013-2025" -ForegroundColor Cyan

$FailedYears = @(2025,2024,2023,2022,2021,2020,2019,2018,2017,2016,2015,2014,2013)

foreach ($Year in $FailedYears) {
    Write-Host "--- Season end $Year ---" -ForegroundColor Yellow
    uv run nba-vault ingestion ingest-players --season-end-year $Year 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[WARN] ingest-players failed for $Year" -ForegroundColor Red
    } else {
        Write-Host "[OK] Season $Year done" -ForegroundColor Green
    }
}

Write-Host "[OK] Recovery complete" -ForegroundColor Green
