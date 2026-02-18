# NBA Vault — Data Sourcing Reference

Canonical reference for every data source used in NBA Vault, including era gates,
intentionally-excluded data, and the ingestor that owns each domain.

---

## 1. Source Overview

| Source | Type | Coverage | Rate limits |
|---|---|---|---|
| **NBA.com Stats API** | REST/JSON | 1996-97 to present (most endpoints) | Unofficial; ~1 req/s |
| **Basketball Reference** | HTML scrape | 1946-47 to present | ~3 s between requests |
| **ESPN** | HTML scrape | Current season injuries | ~2 s between requests |
| **Rotowire** | HTML scrape | Current season injuries | ~2 s between requests |

---

## 2. Ingestor Catalogue (20 ingestors)

### 2.1 Reference / Seed Data

| Ingestor key | Class | Source | Era gate | CLI command |
|---|---|---|---|---|
| `players` | `PlayersIngestor` | Basketball Reference | 1946-47+ | `nba-vault ingestion ingest-players` |
| `franchises` | `FranchiseIngestor` | NBA.com `franchiseHistory` | All | `nba-vault game-data ingest-franchises` |
| `seasons` | `SeasonIngestor` | NBA.com `leagueGameLog` | All | `nba-vault game-data ingest-seasons` |
| `player_bio` | `PlayerBioIngestor` | NBA.com `commonPlayerInfo` | All | `nba-vault game-data ingest-player-bio` |
| `coaches` | `CoachIngestor` | NBA.com `commonTeamRoster` | All | `nba-vault game-data ingest-coaches` |

### 2.2 Games & Officials

| Ingestor key | Class | Source | Era gate | CLI command |
|---|---|---|---|---|
| `game_schedule` | `GameScheduleIngestor` | NBA.com `leagueGameLog` | All | `nba-vault game-data ingest-schedule` |
| `game_officials` | `GameOfficialIngestor` | NBA.com `boxScoreSummaryV2` | All | `nba-vault game-data ingest-officials` |

### 2.3 Box Scores

| Ingestor key | Class | Source | Era gate | CLI command |
|---|---|---|---|---|
| `box_scores_traditional` | `BoxScoreTraditionalIngestor` | NBA.com `boxScoreTraditionalV2` | 1996-97+ | `nba-vault game-data ingest-box-scores` |
| `box_scores_advanced` | `BoxScoreAdvancedIngestor` | NBA.com `boxScoreAdvancedV2` | 1996-97+ | `nba-vault game-data ingest-box-scores-advanced` |
| `box_scores_hustle` | `BoxScoreHustleIngestor` | NBA.com `boxScoreHustleV2` | **2015-16+** | `nba-vault game-data ingest-box-scores-hustle` |

### 2.4 Play-by-Play & Shot Charts

| Ingestor key | Class | Source | Era gate | CLI command |
|---|---|---|---|---|
| `play_by_play` | `PlayByPlayIngestor` | NBA.com `playByPlayV2` | 1996-97+ | `nba-vault game-data ingest-pbp` |
| `shot_chart` | `ShotChartIngestor` | NBA.com `shotChartDetail` | 1996-97+ | `nba-vault game-data ingest-shot-charts` |

### 2.5 Player Stats & Tracking

| Ingestor key | Class | Source | Era gate | CLI command |
|---|---|---|---|---|
| `player_season_stats` | `PlayerSeasonStatsIngestor` | NBA.com `playerCareerStats` | All | `nba-vault game-data ingest-season-stats` |
| `player_tracking` | `PlayerTrackingIngestor` | NBA.com `playerDashPtStats` | **2013-14+** | `nba-vault advanced-stats ingest-tracking` |

### 2.6 Team Stats

| Ingestor key | Class | Source | Era gate | CLI command |
|---|---|---|---|---|
| `lineups` | `LineupsIngestor` | NBA.com `leagueDashLineups` | 1996-97+ | `nba-vault advanced-stats ingest-lineups` |
| `team_other_stats` | `TeamOtherStatsIngestor` | NBA.com `teamDashboardByGeneralSplits` | 1996-97+ | `nba-vault advanced-stats ingest-team-other-stats` |
| `team_advanced_stats` | `TeamAdvancedStatsIngestor` | NBA.com `leagueDashTeamStats` | 1996-97+ | `nba-vault advanced-stats ingest-team-advanced-stats` |

### 2.7 Draft

| Ingestor key | Class | Source | Era gate | CLI command |
|---|---|---|---|---|
| `draft` | `DraftIngestor` | NBA.com `draftHistory` | All (1947+) | `nba-vault game-data ingest-draft` |
| `draft_combine` | `DraftCombineIngestor` | NBA.com `draftCombineAnthro` + `draftCombineDrills` | **2000+** | `nba-vault game-data ingest-draft-combine` |

### 2.8 Awards & Injuries

| Ingestor key | Class | Source | Era gate | CLI command |
|---|---|---|---|---|
| `awards` | `AwardsIngestor` | NBA.com `playerAwards` | All | `nba-vault game-data ingest-awards` |
| `injuries` | `InjuryIngestor` | ESPN / Rotowire (scrape) | Current season | `nba-vault scrapers ingest-injuries` |

### 2.9 Intentionally Excluded

| Domain | Reason | Ingestor |
|---|---|---|
| **Player contracts / salaries** | Excluded per PRD §3 (data licensing) | `contracts` — stub; all methods raise `NotImplementedError` |

---

## 3. Era Gates Reference

Era gates are enforced inside each ingestor's `fetch()` method via
`check_data_availability(entity_type, season_year)`.

| Era gate | First season | Reason |
|---|---|---|
| Hustle stats | 2015-16 | NBA.com only tracks hustle metrics from this season |
| Player tracking | 2013-14 | SportVU cameras installed in all arenas |
| Box score / PBP | 1996-97 | NBA.com digital records begin |
| Draft combine | 2000 | Combine measurements not available before 2000 |
| All other | 1946-47 | Basketball Reference covers full BAA/NBA/ABA history |

---

## 4. Data Validation Strategy

Every ingestor validates raw API/scrape output through Pydantic v2 models
(`nba_vault/models/entities.py`) before writing to SQLite.

| Validation layer | Mechanism |
|---|---|
| **Type coercion** | Pydantic v2 strict-ish mode; e.g. `int` fields reject non-numeric strings |
| **Foreign-key pre-check** | `require_fk(conn, table, col, val)` before each upsert |
| **Quarantine** | Rows that fail validation are written to `data/quarantine/` as JSON |
| **Audit trail** | Every ingest call writes a row to `ingestion_audit` (status, row count, ts) |
| **Fuzzy name matching** | Injury ingestor resolves player names via `difflib.get_close_matches(cutoff=0.85)` |

---

## 5. Rate Limiting & Caching

- All NBA.com requests go through `NBAStatsClient`, which uses `RateLimiter` (1 req/s default).
- Basketball Reference scrapes use a 3-second delay between requests.
- Raw API responses are cached in `cache/` directory (configurable TTL via settings).
- Cache key = `{endpoint}_{params_hash}`.

---

## 6. Season & ID Conventions

| Convention | Format | Example |
|---|---|---|
| Season string (NBA.com API) | `YYYY-YY` | `2023-24` |
| Season ID (database) | `int` (start year) | `2023` |
| Game ID | 10-char string | `0022300001` |
| Team ID | `int` (NBA.com) | `1610612747` (Lakers) |
| Player ID | `int` (NBA.com) | `2544` (LeBron James) |
| Player ID (Bball Ref) | `str` | `jamesle01` |
