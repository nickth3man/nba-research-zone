# NBA Vault Data Ingestion Guide

This guide covers the complete data ingestion system for NBA Vault, including all new ingestors for advanced statistics, tracking data, lineups, injuries, and contracts.

## Overview

NBA Vault uses a modular ingestion framework with the following components:

- **BaseIngestor**: Abstract base class for all ingestors
- **Ingestor Registry**: Automatic registration and discovery of ingestors
- **NBA.com Stats Client**: Wrapper for NBA.com Stats API
- **Basketball Reference Client**: Wrapper for Basketball Reference scraping
- **Specialized Ingestors**: Individual ingestors for each data type

## Available Ingestors

### 1. Players Ingestor (`players`)

Ingests basic player information and statistics from Basketball Reference.

**Data Source**: Basketball Reference (basketball-reference.com)
**Data Availability**: 1946-present
**Tables**: `player`

**Usage**:
```bash
# Ingest all players from 2023-24 season
nba-vault ingest-players --season-end-year 2024

# Ingest specific player
nba-vault ingest-players --player-id jamesle01
```

**Python API**:
```python
from nba_vault.ingestion import create_ingestor
from nba_vault.schema.connection import get_db_connection

conn = get_db_connection()
ingestor = create_ingestor("players")
result = ingestor.ingest("season", conn, season_end_year=2024)
```

### 2. Player Tracking Ingestor (`player_tracking`)

Ingests player tracking data including speed, distance, touches, and drives from NBA.com Stats API.

**Data Source**: NBA.com Stats API
**Data Availability**: 2013-14 season onwards
**Tables**: `player_game_tracking`

**Fields**:
- Distance covered (total, offensive, defensive)
- Speed (average, maximum)
- Touches (total, catch & shoot, paint, post-up)
- Drives and points on drives
- Pull-up shots

**Usage**:
```bash
# Ingest tracking data for specific player
nba-vault ingest-tracking --player-id 2544 --season 2023-24

# Ingest tracking data for all players on a team
nba-vault ingest-tracking --team-id 1610612747 --season 2023-24

# Ingest playoff tracking data
nba-vault ingest-tracking --player-id 2544 --season 2023-24 --season-type "Playoffs"
```

**Python API**:
```python
ingestor = create_ingestor("player_tracking")
result = ingestor.ingest("2544", conn, season="2023-24", season_type="Regular Season")
```

### 3. Lineups Ingestor (`lineups`)

Ingests lineup combination data including performance metrics for specific player groups.

**Data Source**: NBA.com Stats API
**Data Availability**: Varies by season (more complete for recent seasons)
**Tables**: `lineup`, `lineup_game_log`

**Fields**:
- Player combinations (5 players)
- Minutes played together
- Offensive, defensive, and net ratings
- Points scored and allowed
- Possessions

**Usage**:
```bash
# Ingest all lineups in the league
nba-vault ingest-lineups --scope league --season 2023-24

# Ingest lineups for specific team
nba-vault ingest-lineups --scope team --team-id 1610612747 --season 2023-24

# Ingest lineups for specific game
nba-vault ingest-lineups --scope game:0022300001 --season 2023-24
```

**Python API**:
```python
ingestor = create_ingestor("lineups")
result = ingestor.ingest("league", conn, season="2023-24")
```

### 4. Team Other Stats Ingestor (`team_other_stats`)

Ingests team game "other stats" including paint points, fast break points, and more.

**Data Source**: NBA.com Stats API (Box Score Summary)
**Data Availability**: Varies by season
**Tables**: `team_game_other_stats`

**Fields**:
- Points in the paint
- Second chance points
- Fast break points
- Largest lead
- Lead changes
- Times tied
- Team turnovers and rebounds
- Points off turnovers

**Usage**:
```bash
# Ingest other stats for specific game
nba-vault ingest-team-other-stats --game-id 0022300001

# Ingest other stats for all games in a season (team)
nba-vault ingest-team-other-stats --team-id 1610612747 --season 2023-24
```

**Python API**:
```python
ingestor = create_ingestor("team_other_stats")
result = ingestor.ingest("0022300001", conn, season="2023-24")
```

### 5. Team Advanced Stats Ingestor (`team_advanced_stats`)

Ingests advanced team statistics including offensive/defensive ratings, pace, and four factors.

**Data Source**: NBA.com Stats API
**Data Availability**: Varies by season
**Tables**: `team_season_advanced`

**Fields**:
- Offensive and defensive ratings
- Net rating
- Pace factor
- Effective field goal percentage
- Turnover percentage
- Offensive rebounding percentage
- Free throw rate
- Three-point rate
- True shooting percentage

**Usage**:
```bash
# Ingest advanced stats for all teams
nba-vault ingest-team-advanced-stats --scope league --season 2023-24

# Ingest advanced stats for specific team
nba-vault ingest-team-advanced-stats --scope team --team-id 1610612747 --season 2023-24

# Ingest four factors data
nba-vault ingest-team-advanced-stats --scope league --season 2023-24 --measure-type "Four Factors"
```

**Python API**:
```python
ingestor = create_ingestor("team_advanced_stats")
result = ingestor.ingest("league", conn, season="2023-24", measure_type="Advanced")
```

### 6. Injury Ingestor (`injuries`)

Ingests player injury data from various sources including ESPN, Rotowire, and NBA.com.

**Data Source**: ESPN, Rotowire, NBA.com (web scraping)
**Data Availability**: Varies by source and season
**Tables**: `injury`

**Fields**:
- Player and team
- Injury date
- Injury type and body part
- Status (out, day-to-day, questionable, etc.)
- Games missed
- Expected return date

**Usage**:
```bash
# Ingest all injuries from ESPN
nba-vault ingest-injuries --source espn

# Ingest injuries for specific team
nba-vault ingest-injuries --team LAL --source espn

# Ingest injuries from Rotowire
nba-vault ingest-injuries --source rotowire
```

**Python API**:
```python
ingestor = create_ingestor("injuries")
result = ingestor.ingest("all", conn, source="espn")
```

### 7. Contract Ingestor (`contracts`)

Ingests player contract data from sources like RealGM and Spotrac.

**Data Source**: RealGM, Spotrac (web scraping)
**Data Availability**: Varies by source and season
**Tables**: `player_contract`

**Fields**:
- Player and team
- Contract years (season start and end)
- Salary amount per season
- Contract type (rookie, veteran, MLE, etc.)
- Option types (player option, team option, ETO)
- Guaranteed money and cap hit

**Usage**:
```bash
# Ingest all contracts from RealGM
nba-vault ingest-contracts --source realgm

# Ingest contracts for specific team
nba-vault ingest-contracts --team "Los Angeles Lakers" --source realgm

# Ingest contracts from Spotrac
nba-vault ingest-contracts --source spotrac
```

**Python API**:
```python
ingestor = create_ingestor("contracts")
result = ingestor.ingest("all", conn, source="realgm")
```

## NBA.com Stats API Client

The `NBAStatsClient` provides direct access to NBA.com Stats API endpoints:

```python
from nba_vault.ingestion import NBAStatsClient

client = NBAStatsClient()

# Get player tracking data
tracking_data = client.get_player_tracking(
    player_id=2544,
    season="2023-24",
    season_type="Regular Season"
)

# Get team lineups
lineup_data = client.get_team_lineups(
    team_id=1610612747,
    season="2023-24"
)

# Get box score summary
boxscore_data = client.get_box_score_summary(
    game_id="0022300001"
)

# Get team advanced stats
advanced_data = client.get_team_advanced_stats(
    team_id=1610612747,
    season="2023-24",
    measure_type="Advanced"
)
```

## Data Availability

### By Data Type

| Data Type | Source | Earliest Season | Notes |
|-----------|---------|-----------------|-------|
| Basic Player Stats | Basketball Reference | 1946-47 | Fairly complete |
| Player Tracking | NBA.com | 2013-14 | Player movement data |
| Lineups | NBA.com | Varies | More complete for recent seasons |
| Team Other Stats | NBA.com | Varies | Box score data |
| Team Advanced Stats | NBA.com | Varies | Advanced metrics |
| Injuries | ESPN/Rotowire | Varies | Inconsistent historical data |
| Contracts | RealGM/Spotrac | Varies | Financial data often incomplete |

### Season Format

All ingestors use the "YYYY-YY" season format (e.g., "2023-24" for the 2023-24 season).

- **Regular Season**: Default season type
- **Playoffs**: Postseason data
- **Pre Season**: Exhibition games

## Error Handling

All ingestors implement comprehensive error handling:

1. **Retry Logic**: Automatic retries with exponential backoff for transient failures
2. **Validation**: Pydantic models validate all data before database insertion
3. **Quarantine**: Failed validations are logged (future: written to quarantine directory)
4. **Audit Trail**: All ingestion attempts are logged to `ingestion_audit` table

## Rate Limiting

The ingestors respect rate limits of external APIs:

- **NBA.com Stats API**: ~6 requests/minute (conservative)
- **Basketball Reference**: ~8 requests/minute (with caching)
- **Web Scraping**: Respects `robots.txt` and implements delays

## Caching

All ingestors use content caching to minimize API calls:

- Cache directory: `data/cache/`
- Cache keys include: endpoint, parameters, and timestamps
- Cached data respects `Cache-Control` headers when available

## Database Operations

### Insert vs Update

Ingestors automatically determine whether to insert or update:

- **Insert**: New records (based on primary key lookup)
- **Update**: Existing records (update all fields)

### Transaction Safety

All database operations use proper transactions:

```python
conn = get_db_connection()
try:
    result = ingestor.ingest(entity_id, conn)
    conn.commit()  # Commit on success
except Exception as e:
    conn.rollback()  # Rollback on failure
    raise
finally:
    conn.close()
```

## Testing

All ingestors have comprehensive test coverage:

```bash
# Run all tests
uv run pytest

# Run specific ingestor tests
uv run pytest tests/test_new_ingestors.py

# Run with coverage
uv run pytest --cov=nba_vault/ingestion --cov-report=html
```

## Best Practices

### 1. Incremental Ingestion

For regular updates, use incremental ingestion:

```bash
# Ingest today's games
nba-vault ingest --mode incremental
```

### 2. Backfill Historical Data

For complete historical coverage, use full backfill:

```bash
# Backfill all seasons
nba-vault ingest --mode full --start-season 1946 --end-season 2024 --workers 4
```

### 3. Handle Data Gaps

Historical data has gaps. Always check availability:

```python
# Check if tracking data is available for season
season_year = int(season.split("-")[0])
if season_year < 2013:
    raise ValueError("Player tracking data not available before 2013-14")
```

### 4. Monitor Ingestion Health

Check the ingestion audit table:

```sql
-- Recent ingestion status
SELECT
    entity_type,
    status,
    COUNT(*) as count,
    MAX(ingested_at) as last_ingested
FROM ingestion_audit
GROUP BY entity_type, status
ORDER BY entity_type, status;

-- Recent failures
SELECT *
FROM ingestion_audit
WHERE status != 'SUCCESS'
ORDER BY ingested_at DESC
LIMIT 20;
```

## Troubleshooting

### Common Issues

1. **Rate Limiting**: If you see 429 errors, reduce concurrent workers
2. **Validation Errors**: Check quarantine directory for raw data
3. **Missing Data**: Historical data is incomplete; check availability tables
4. **API Changes**: NBA.com API endpoints change without notice; check for updates

### Debug Mode

Enable debug logging:

```bash
# Set log level
export LOG_LEVEL=DEBUG

# Run with verbose output
nba-vault ingest-tracking --player-id 2544 --season 2023-24
```

### Database Status

Check database status:

```bash
nba-vault status
```

## Future Enhancements

Planned improvements to the ingestion system:

1. **Parallel Ingestion**: Multi-threaded ingestion for large backfills
2. **Delta Updates**: Only ingest changed data
3. **Real-time Updates**: WebSocket support for live game data
4. **Expanded Sources**: Additional data providers (Sportradar API, etc.)
5. **Data Quality Metrics**: Automated quality scoring and validation
6. **Historical Backfill**: Improved support for incomplete historical data

## Contributing

To add a new ingestor:

1. Inherit from `BaseIngestor`
2. Implement `fetch()`, `validate()`, and `upsert()` methods
3. Register with `@register_ingestor` decorator
4. Add Pydantic models to `models/advanced_stats.py`
5. Create database migration if needed
6. Add CLI command to `cli.py`
7. Write comprehensive tests
8. Update this documentation

## References

- [NBA.com Stats API](https://github.com/swar/nba_api)
- [Basketball Reference](https://www.basketball-reference.com/)
- [ESPN NBA](https://www.espn.com/nba/)
- [RealGM](https://basketball.realgm.com/)
- [Spotrac](https://www.spotrac.com/nba/)
