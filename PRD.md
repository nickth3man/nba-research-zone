```markdown
# Product Requirements Document

**Project:** NBAVault — Comprehensive Historical NBA Database
**Version:** 0.1
**Author:** Nick
**Date:** 2026-02-17
**Status:** Draft

---

## 1. Problem Statement

> What problem exists, and why does the current landscape fail to solve it?

- **The gap:** No single open-source, self-hostable NBA database captures the full breadth of historical league data — seasons, franchises, players, coaches, officials, games, play-by-play, shot charts, draft history, awards, transactions, and advanced metrics — in a unified, query-optimized, analyst-ready format. Existing projects (wyattowalsh/nbadb, mpope9/nba-sql, databasebasketball.com) each cover a slice: some stop at box scores, some omit coaches and officials entirely, some are Postgres-only, most lag on schema coherence, and none support OLAP-style analytical workloads natively.
- **Why now:** The `swar/nba_api` Python library (3.4k stars, actively maintained) exposes 100+ stats.nba.com endpoints including play-by-play, shot charts, referee assignments, coaching records, and draft data going back to the 1946–47 BAA/NBA founding season. DuckDB's rise as an embedded OLAP engine makes it feasible for a single engineer to build a database that answers both transactional lookups and analytical fan-out queries on a laptop without spinning up a server. The combination of SQLite (source of truth) + DuckDB (analytics layer) has become a well-understood, operationally simple open-source pattern.
- **Who cares:** Sports analysts and data journalists who need reliable historical context for comparative research; fantasy sports developers who need daily-updating box scores; application developers building basketball tools who want a local-first data layer; academic researchers studying economics, sociology, and physiology through the lens of professional basketball.

---

## 2. Core Goals

> The two or three non-negotiable outcomes this project must achieve.

| # | Goal | Definition of Done |
|---|------|--------------------|
| 1 | **Complete coverage** — every entity class in NBA history representable in the schema | All 78 seasons (1946–47 to present), all ~5,000 players, all 30 current + ~30 historical franchises, coaches, officials, every regular-season and playoff game, draft picks back to 1947, and major awards populated |
| 2 | **Analyst-ready dual-engine architecture** — SQLite for durability, DuckDB for analytics | Single CLI command produces a `.sqlite` master file and a `.duckdb` analytical file from the same source data; DuckDB views expose pre-joined analytical tables |
| 3 | **Automated incremental updates** — daily sync without full re-ingest** | A scheduler (APScheduler or cron) runs nightly, detects completed games since last run, inserts new rows, and vacuums; full historical backfill is a one-time operation |

---

## 3. Explicit Non-Goals

> What this project will not do. Capturing this early prevents scope creep.

- [ ] WNBA, G-League, or international league data (deferred; schema will be extensible but not populated)
- [ ] Real-time in-game data streaming or live score API server
- [ ] A user-facing web application or REST API endpoint layer
- [ ] Salary and contract data (legally ambiguous; intentionally excluded)
- [ ] Video or image assets (media URLs stored as strings but assets not downloaded)
- [ ] Predictive modeling or ML pipelines (consumer of this database, not part of it)
- [ ] Any proprietary metric reproduction (Second Spectrum tracking data, Synergy, etc.)
- [ ] PostgreSQL support (out of scope per constraint; SQLite + DuckDB only)

---

## 4. User Personas

| Persona | Context | Core Need | Acceptable Trade-off |
|---------|---------|-----------|----------------------|
| **Analytics Engineer** | Python or SQL notebook environment; comfort with CLI tooling | Query entire NBA history with OLAP-style aggregations in < 2s; join players ↔ teams ↔ games without schema gymnastics | Willing to run a 6–12 hr initial backfill; comfortable with occasional API rate-limit throttling |
| **Application Developer** | Building a web or mobile app backed by local data | Portable `.sqlite` file that can be bundled or served; stable primary keys; FK integrity enforced | Accepts that schema versions require a migration script; can live without sub-second write latency |
| **Sports Journalist / Researcher** | Jupyter notebooks, R, or Excel via ODBC | Flat, denormalized CSV exports and pre-built DuckDB views for quick exploration | Happy with monthly rather than daily freshness for deep-historical queries |
| **Fantasy Sports Developer** | Automated pipeline consuming game results | Daily box-score updates, reliable player and team IDs stable across seasons | Tolerates 24-hour data lag; does not need play-by-play granularity |
| **Open-Source Contributor** | Local dev environment with Python and SQL experience; does not want to run a full backfill to test a patch | Clear extension points (`BaseIngestor`, DuckDB view `.sql` files), a dev-mode dataset covering a single recent season, Pydantic model auto-docs | Accepts a 1–2 season dev dataset; does not need full historical coverage locally |

---

## 5. Technical Constraints

- **Runtime environment:** Local machine (Linux / macOS / Windows WSL); Python 3.11+ process; no cloud infrastructure required; all data stored to local filesystem
- **Dependency restrictions:** No PostgreSQL (hard constraint). All storage via SQLite (≥ 3.45) and DuckDB (≥ 1.0). No proprietary data vendors. No paid APIs.
- **Data / privacy requirements:** All data sourced from publicly accessible endpoints (stats.nba.com, Basketball-Reference.com scrapers). No PII beyond publicly known biographical data (DOB, hometown, college). Data must be storable and redistributable under open-source-compatible terms; users are responsible for compliance with stats.nba.com ToS in their jurisdiction.
- **Size / performance budget:** Initial full historical SQLite database estimated at 15–40 GB (including play-by-play and shot chart rows). DuckDB analytical file estimated at 5–15 GB with columnar compression. Incremental daily update must complete in under 10 minutes on commodity hardware (8-core CPU, 16 GB RAM, SSD). Query latency target: simple lookups < 50 ms in SQLite; analytical aggregations over full history < 5 s in DuckDB.
- **API rate limiting:** stats.nba.com enforces informal rate limits (~10 requests/min sustained); the ingestion pipeline must implement exponential back-off with jitter and honor `Retry-After` headers.
- **Dependency management:** All Python dependencies must be pinned in a lock file (`uv.lock` or `poetry.lock`). `nba_api`, `duckdb`, and `pydantic` require exact version pins. Any upgrade to `nba_api` must be validated against the ingestion regression suite before deployment, since stats.nba.com response-shape drift is the primary production risk and `nba_api` patches may introduce their own breaking changes mid-backfill.

---

## 6. Architecture Overview

### 6.1 Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                      Ingestion Layer                        │
│  ┌──────────────┐   ┌───────────────────┐  ┌────────────┐  │
│  │  nba_api     │   │  BR Scraper       │  │ Static CSV │  │
│  │  (swar/      │   │  (httpx +         │  │ Seeds      │  │
│  │   nba_api)   │   │   BeautifulSoup4) │  │ (awards,   │  │
│  └──────┬───────┘   └─────────┬─────────┘  │  ABA era)  │  │
│         └──────────────┬──────┘            └─────┬──────┘  │
│                        ▼                         │          │
│              ┌──────────────────┐                │          │
│              │  Raw JSON Cache  │◄───────────────┘          │
│              │  (filesystem,    │                            │
│              │   content-hash   │                            │
│              │   keyed)         │                            │
│              └────────┬─────────┘                            │
└───────────────────────┼──────────────────────────────────────┘
                        │
                        ▼
┌───────────────────────────────────────────────────────────────┐
│                   Transform & Load Layer                      │
│   Pydantic validation → schema normalization → upsert logic   │
│                        │                                      │
│               ┌────────▼────────┐                             │
│               │   SQLite        │  ← source-of-truth store    │
│               │   (nba.sqlite)  │    FK-enforced, WAL mode    │
│               └────────┬────────┘                             │
└────────────────────────┼──────────────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────────────────────────────────┐
│                    Analytics Layer                             │
│   DuckDB reads SQLite via sqlite_scan() extension             │
│   Views (re-materialized explicitly after each update):       │
│   player_career_stats, franchise_timeline, season_leaders,    │
│   game_officials, head_to_head_records, shot_zone_agg         │
│                  │                                             │
│        ┌─────────▼──────────┐   ┌──────────────────────┐     │
│        │  nba.duckdb         │   │  Parquet exports     │     │
│        │  (analytical file)  │   │  (per entity class)  │     │
│        └─────────────────────┘   └──────────────────────┘     │
└────────────────────────────────────────────────────────────────┘
```

### 6.2 Data / Entity Model

The canonical NBA entity universe and their identifiers:

| Entity Class | Canonical ID Scheme | Notes |
|---|---|---|
| **Season** | `{start_year}` integer (e.g., `2024` for 2024–25) | Covers BAA 1946 → present |
| **League** | `{league_code}` ENUM (`NBA`, `ABA`, `BAA`) | ABA 1967–76 merged franchise records |
| **Franchise** | `franchise_id` integer (NBA.com canonical) | Persists across relocations/renames |
| **Team** | `team_id` integer (NBA.com season-specific) | One franchise can have multiple historical team records |
| **Player** | `player_id` integer (NBA.com canonical) | Stable across trades; historical players backfilled |
| **Coach** | `coach_id` integer (NBA.com) | Includes assistants with designation flag |
| **Official** | `official_id` integer (NBA.com) | Available from approx. 1990–91 onward |
| **Game** | `game_id` 10-character string (NBA.com format) | `{season_code}{game_type_code}{sequence}` |
| **Draft** | Composite `(draft_year, round, pick)` | Back to 1947 BAA draft |
| **Award** | Composite `(award_type, season, player_id)` | MVP, ROY, DPOY, All-NBA, All-Star, etc. |
| **Transaction** | `transaction_id` UUID generated | Trades, signings, waivers where data available |

**Known ambiguities and edge cases:**

- The BAA (1946–49) and ABA (1967–76) entities must be modeled as distinct leagues with a `league_id` FK to avoid conflating statistics across eras with different rules and contexts.
- Franchise continuity requires a `franchise_team_map` bridge table: the Oklahoma City Thunder (franchise_id = 1228) is the same franchise as the Seattle SuperSonics; team_id differs per season.
- Players who played in both the ABA and NBA need cross-league career aggregation views that clearly label era.
- `game_id` on stats.nba.com uses a 10-digit scheme; pre-modern game IDs must be synthetically generated following the same pattern for internal consistency.
- Coaching records must distinguish head coach tenure from interim head coach stints.

### 6.3 Pipeline / Routing Logic

**Ingestion routing by entity type:**

| Entity | Primary Source | Fallback Source | Frequency |
|---|---|---|---|
| Season metadata | `nba_api.SeasonYear` | Static seed file | One-time + annual |
| Franchise / team master | `nba_api.FranchiseHistory` | Basketball-Reference | One-time |
| Player biography | `nba_api.CommonPlayerInfo` | Basketball-Reference player pages | One-time + daily additions |
| Game schedule | `nba_api.LeagueGameFinder` | `nba_api.ScoreboardV2` | Daily |
| Box score (team) | `nba_api.BoxScoreTraditionalV2` | — | Per game |
| Box score (player) | `nba_api.BoxScoreTraditionalV2` | — | Per game |
| Advanced box score | `nba_api.BoxScoreAdvancedV2` | — | Per game |
| Hustle stats | `nba_api.BoxScoreHustleV2` | — | Per game (2015+ era) |
| Play-by-play | `nba_api.PlayByPlayV2` | `nba_api.PlayByPlayV3` | Per game |
| Shot chart | `nba_api.ShotChartDetail` | — | Per game |
| Officials per game | `nba_api.BoxScoreSummaryV2` | — | Per game |
| Season player stats | `nba_api.PlayerCareerStats` | — | Per player per season |
| Coaching staff | `nba_api.CommonTeamRoster` (coaches flag) | Basketball-Reference | Per team per season |
| Draft | `nba_api.DraftHistory` | Basketball-Reference | One-time + annual |
| Awards | `nba_api.PlayerAwards` | Basketball-Reference | Per player; annual |
| Transactions | Basketball-Reference transactions page | realgm.com | Annual |

**Failure handling:**

- Endpoint returns empty dataset → log warning, mark game_id in `ingestion_audit` table with `status='EMPTY'`, skip without failing the run.
- HTTP 429 or 5xx → exponential back-off starting at 30 s, max 5 retries, then mark `status='FAILED'` in audit table and continue.
- Schema drift (NBA.com silently changing response shape) → Pydantic `model_validate` raises `ValidationError`; pipeline logs the raw JSON to a quarantine directory and continues; alert is raised for human review.
- Missing historical data (pre-1990 play-by-play genuinely unavailable) → `NULL` fields with a `data_availability_flags` bitmask column per entity.

---

## 7. Tool & Library Selection

| Capability | Chosen Tool | Alternatives Considered | Decision Rationale |
|---|---|---|---|
| NBA stats API client | `swar/nba_api` (Python, MIT) | Raw `httpx` calls, `py-ball` | 3.4k stars, 100+ endpoint wrappers, actively maintained, handles headers/rate-limiting boilerplate |
| HTML scraping (BR fallback) | `httpx` + `BeautifulSoup4` | `playwright`, `scrapy` | Lightweight; Basketball-Reference is server-rendered HTML; no JS execution needed |
| Primary storage | SQLite 3.45+ (WAL mode) | MySQL, MariaDB, PostgreSQL | Zero-infrastructure; portable single file; WAL mode enables concurrent reads; excluded by constraint for Postgres |
| Analytical query engine | DuckDB 1.0+ | Apache Arrow, Pandas | Columnar storage; native Parquet and SQLite read support; SQL-native; 10–100× faster than SQLite on analytical aggregations; embedded (no server) |
| Data validation | Pydantic v2 | `dataclasses`, `marshmallow` | Performance; ergonomic model definitions; V2 is significantly faster for bulk validation |
| Schema migrations | `yoyo-migrations` | `alembic` | Alembic supports SQLite but is most mature against Postgres; yoyo-migrations uses plain SQL files and is SQLite-native. **Risk:** yoyo-migrations has ~200 GitHub stars and limited recent maintenance activity. Given that migrations run against a 40 GB production database, monitor library health actively; fallback plan is raw `sqlite3` script files. |
| Scheduling | `APScheduler` (in-process) | `cron`, `Airflow`, `Prefect` | Zero-infrastructure; no daemon; sufficient for single-machine daily cadence |
| CLI interface | `Typer` | `click`, `argparse` | Auto-generates `--help`; type-annotated; minimal boilerplate |
| Parquet export | `pyarrow` | `pandas.to_parquet`, `fastparquet` | DuckDB uses PyArrow internally; consistent serialization |
| Logging | `structlog` | `loguru`, stdlib `logging` | Structured JSON logs enable audit trail parsing without regex |

**Build vs. buy decisions:**

| Component | Decision | Justification |
|---|---|---|
| Rate-limiting / retry logic | Build (thin wrapper) | `nba_api` has basic rate-limiting but doesn't implement exponential backoff with jitter; 30-line custom implementation sufficient |
| Cache layer (raw JSON) | Build (content-hash filesystem cache) | Prevents re-fetching completed historical games; avoids paid caching infrastructure; ~40-line implementation |
| Franchise continuity resolver | Build | No existing open-source library maps NBA.com team_id history to canonical franchise_id; requires a seeded lookup table and merge logic |
| Ingestion audit table | Build | Lightweight `ingestion_audit` SQLite table tracking per-entity ingestion status, timestamp, and row count; replaces complex workflow orchestration for this scale |
| DuckDB analytical views | Build | Domain-specific denormalization for NBA analytics; cannot be satisfied by a generic library |

---

## 8. Functional Requirements

### 8.1 Must Have (P0)

- [ ] Full franchise history table mapping every team name, city, arena, division, conference, and season range to a canonical `franchise_id`
- [ ] Complete player biography table covering all ~5,000 NBA/ABA/BAA players: name, DOB, birthplace, height, weight, position(s), college, draft info, active flag
- [ ] Season-level player statistics (traditional, advanced, per-game, per-36, per-100-possessions) for every player in every season back to 1946–47
- [ ] Game-level records for all regular season and playoff games: date, teams, score, arena, attendance, game type, overtime periods
- [ ] Player box scores (traditional + advanced) for every game in the database
- [ ] Team box scores (traditional + advanced) for every game in the database
- [ ] Head coach records per team per season including W/L, tenure start/end dates, interim flag
- [ ] Draft history table: pick number, round, team, player, original team (for traded picks), year — back to 1947
- [ ] Award records: MVP, Rookie of the Year, Defensive Player of the Year, Sixth Man, Most Improved, All-NBA teams (1st/2nd/3rd), All-Defensive teams, All-Star selections, Finals MVP, back to first availability
- [ ] Officials/referee table with per-game assignment records (available from ~1990–91)
- [ ] `ingestion_audit` table tracking the status of every entity-level fetch operation
- [ ] DuckDB analytical database with at minimum: `player_career_stats`, `franchise_timeline`, `season_leaders`, `game_officials`, `head_to_head_records`, `shot_zone_agg` views
- [ ] CLI with commands: `ingest --full`, `ingest --incremental`, `export --format [parquet|csv|duckdb]`, `migrate`, `validate`
- [ ] `nba-vault validate` must verify: (a) FK referential integrity across all constrained relationships, (b) game coverage completeness per season — actual row count in `player_game_log` vs. expected based on `ingestion_audit` records, (c) `data_availability_flags` internal consistency (if the `SHOT_CHART` bit is set for a `game_id`, at least one row must exist in `shot_chart` for that game), (d) schema version alignment between `yoyo-migrations` history table and live schema
- [ ] Data availability flags (`data_availability_flags` bitmask) on game and player records documenting which data tiers are present for each row; bit definitions enumerated in `data_availability_flag_def` table

### 8.2 Should Have (P1)

- [ ] Play-by-play records for all games with available data (~1996–97 onward): event type, team, player, period, clock, score at event
- [ ] Shot chart data for all games with available data (~1996–97 onward): player, shot type, zone, distance, make/miss, x/y coordinates
- [ ] Coaching staff table (assistant coaches) with role designations
- [ ] Player transaction log: trades, signings, waivers, two-way contracts (where source data available)
- [ ] Series-level playoff bracket records with round, matchup, and per-game results
- [ ] ABA-era player and team statistics merged with canonical player_id cross-reference
- [ ] Season standings snapshots (division, conference) by month
- [ ] Player biographical enrichment: nationality, high school, undrafted flag
- [ ] Hustle stats (contested shots, screen assists, deflections) for 2015–16 onward
- [ ] Tracking stats (speed, distance, touches) for 2013–14 onward where available via nba_api
- [ ] Parquet export pipeline producing per-entity-class compressed files

### 8.3 Nice to Have (P2)

- [ ] Pre-game odds / Vegas lines from open historical datasets (if openly available)
- [ ] Arena geolocation coordinates for franchise history mapping
- [ ] Player awards cross-referenced with international (FIBA, EuroLeague) appearances
- [ ] G-League affiliate table linked to NBA franchises
- [ ] FiveThirtyEight RAPTOR metric integration (GPLv3 compatible; CSV ingestion)
- [ ] REST API thin wrapper (FastAPI + read-only SQLite) for local development consumption
- [ ] Web-based schema browser (Datasette) auto-launch for exploration

---

## 9. Cross-Domain Bridging Logic

| Source Domain | Target Domain | Bridging Strategy | Known Quality Loss |
|---|---|---|---|
| BAA (1946–49) entity records | NBA canonical schema | Season `league_id = 'BAA'`; franchise_id assigned using Basketball-Reference historical continuity map | Championship and award coverage incomplete prior to 1950 |
| ABA (1967–76) player stats | NBA career aggregates | Player cross-reference table `player_league_xref(player_id, league_id)`; DuckDB view union-alls with era label | ABA stats excluded from all-time NBA records by convention; view must make this explicit |
| Basketball-Reference player IDs | stats.nba.com player IDs | `player_id_xref(nba_id, bbref_id, basketball_ref_slug)` bridge table seeded from community mapping CSVs | ~3% of historical players may have no nba.com ID (pre-digital era) |
| Raw play-by-play event codes | Semantic event taxonomy | `event_message_type` lookup table (21 event types as defined by NBA.com) | Sub-event granularity varies by season; pre-2000 PBP lacks coordinates |
| Shot chart coordinates (feet) | Court zone taxonomy | Zone assignment via polygon lookup (`shot_zone_basic`, `shot_zone_area`, `shot_zone_range` from NBA.com) | Coordinates recorded as integers (tenths of feet); minor precision loss |

---

## 10. Complete Database Schema

### Core Tables

```sql
-- RECOMMENDED SQLITE PRAGMA CONFIGURATION
-- Apply these on every new connection before any data is written.
-- PRAGMA page_size MUST be set before the first write and cannot be changed afterward without a full VACUUM.
PRAGMA page_size = 16384;    -- 16 KB pages; reduces I/O amplification 30-50% vs default 4 KB on wide analytical tables
PRAGMA journal_mode = WAL;   -- Write-ahead logging; enables concurrent reads during write transactions
PRAGMA synchronous = NORMAL; -- Adequate durability with WAL mode; avoids per-write fsync overhead
PRAGMA foreign_keys = ON;    -- Enforce FK integrity at the SQLite layer
PRAGMA cache_size = -131072; -- 128 MB page cache (negative value = kilobytes)

-- LEAGUE
CREATE TABLE league (
    league_id   TEXT PRIMARY KEY,  -- 'NBA', 'ABA', 'BAA'
    league_name TEXT NOT NULL,
    founded_year INTEGER,
    folded_year  INTEGER           -- NULL if active
);

-- SEASON
-- season_id is a calendar-year integer and must remain UNIQUE (it is the PRIMARY KEY).
-- season_type is intentionally absent: this table represents a calendar year, not a game type.
-- Type differentiation ('Regular Season', 'Playoffs') is carried on game.game_type and
-- player_season_stats.stat_type. Adding season_type here would break PK uniqueness or require
-- a composite PK, creating FK ambiguity across all dependent tables.
CREATE TABLE season (
    season_id       INTEGER PRIMARY KEY,  -- start year e.g. 2024
    league_id       TEXT NOT NULL REFERENCES league(league_id),
    season_label    TEXT NOT NULL,        -- '2024-25'
    games_per_team  INTEGER,
    schedule_start  TEXT,                 -- ISO date
    schedule_end    TEXT,
    champion_franchise_id INTEGER REFERENCES franchise(franchise_id),
    finals_mvp_player_id  INTEGER REFERENCES player(player_id)
);

-- FRANCHISE
CREATE TABLE franchise (
    franchise_id      INTEGER PRIMARY KEY,
    nba_franchise_id  INTEGER UNIQUE,    -- NBA.com canonical ID
    current_team_name TEXT NOT NULL,
    current_city      TEXT NOT NULL,
    abbreviation      TEXT NOT NULL,
    conference        TEXT,
    division          TEXT,
    founded_year      INTEGER,
    league_id         TEXT NOT NULL REFERENCES league(league_id)
);

-- TEAM (season-specific team record)
CREATE TABLE team (
    team_id          INTEGER PRIMARY KEY,  -- NBA.com team_id
    franchise_id     INTEGER NOT NULL REFERENCES franchise(franchise_id),
    season_id        INTEGER NOT NULL REFERENCES season(season_id),
    team_name        TEXT NOT NULL,
    city             TEXT NOT NULL,
    abbreviation     TEXT NOT NULL,
    conference       TEXT NOT NULL,
    division         TEXT NOT NULL,
    arena_name       TEXT,
    arena_capacity   INTEGER,
    owner            TEXT,
    general_manager  TEXT,
    UNIQUE(franchise_id, season_id)
);

-- PLAYER
CREATE TABLE player (
    player_id         INTEGER PRIMARY KEY,   -- NBA.com player_id
    first_name        TEXT NOT NULL,
    last_name         TEXT NOT NULL,
    full_name         TEXT NOT NULL,
    display_name      TEXT,                  -- e.g. "LeBron James"
    birthdate         TEXT,                  -- ISO date
    birthplace_city   TEXT,
    birthplace_state  TEXT,
    birthplace_country TEXT,
    height_inches     REAL,
    weight_lbs        REAL,
    position          TEXT,
    primary_position  TEXT,
    jersey_number     TEXT,                  -- last known
    college           TEXT,
    country           TEXT,
    draft_year        INTEGER,
    draft_round       INTEGER,
    draft_number      INTEGER,
    is_active         INTEGER NOT NULL DEFAULT 1,
    from_year         INTEGER,
    to_year           INTEGER,
    bbref_id          TEXT UNIQUE,           -- Basketball-Reference slug
    data_availability_flags INTEGER NOT NULL DEFAULT 0
);

-- COACH
CREATE TABLE coach (
    coach_id        INTEGER PRIMARY KEY,  -- NBA.com coach_id
    first_name      TEXT NOT NULL,
    last_name       TEXT NOT NULL,
    full_name       TEXT NOT NULL,
    birthdate       TEXT,
    college         TEXT,
    is_active       INTEGER NOT NULL DEFAULT 1
);

-- COACH STINT (per-team per-season assignment)
CREATE TABLE coach_stint (
    stint_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    coach_id         INTEGER NOT NULL REFERENCES coach(coach_id),
    team_id          INTEGER NOT NULL REFERENCES team(team_id),
    season_id        INTEGER NOT NULL REFERENCES season(season_id),
    coach_type       TEXT NOT NULL,      -- 'Head Coach', 'Assistant', 'Interim Head Coach'
    sort_sequence    INTEGER,
    date_hired       TEXT,
    date_fired       TEXT,
    wins             INTEGER,
    losses           INTEGER,
    win_pct          REAL
);

-- OFFICIAL
CREATE TABLE official (
    official_id     INTEGER PRIMARY KEY,  -- NBA.com official_id
    first_name      TEXT NOT NULL,
    last_name       TEXT NOT NULL,
    full_name       TEXT NOT NULL,
    jersey_num      TEXT,
    is_active       INTEGER NOT NULL DEFAULT 1
);

-- ARENA
CREATE TABLE arena (
    arena_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    arena_name      TEXT NOT NULL,
    city            TEXT NOT NULL,
    state           TEXT,
    country         TEXT NOT NULL DEFAULT 'USA',
    capacity        INTEGER,
    opened_year     INTEGER,
    closed_year     INTEGER,
    latitude        REAL,
    longitude       REAL
);

-- GAME
CREATE TABLE game (
    game_id             TEXT PRIMARY KEY,   -- NBA.com 10-char game_id
    season_id           INTEGER NOT NULL REFERENCES season(season_id),
    game_date           TEXT NOT NULL,      -- ISO date
    game_type           TEXT NOT NULL,      -- 'Regular Season', 'Playoffs', 'Pre Season', 'All-Star'
    game_sequence       INTEGER,
    home_team_id        INTEGER NOT NULL REFERENCES team(team_id),
    away_team_id        INTEGER NOT NULL REFERENCES team(team_id),
    home_team_score     INTEGER,
    away_team_score     INTEGER,
    winner_team_id      INTEGER REFERENCES team(team_id),
    overtime_periods    INTEGER NOT NULL DEFAULT 0,
    arena_id            INTEGER REFERENCES arena(arena_id),
    attendance          INTEGER,
    game_duration_mins  INTEGER,
    playoff_round       TEXT,               -- 'First Round', 'Conference Semis', etc.
    playoff_series_id   TEXT,               -- FK to playoff_series
    national_tv         TEXT,               -- 'ESPN', 'TNT', etc.
    data_availability_flags INTEGER NOT NULL DEFAULT 0
);

-- GAME OFFICIAL (junction)
CREATE TABLE game_official (
    game_id         TEXT NOT NULL REFERENCES game(game_id),
    official_id     INTEGER NOT NULL REFERENCES official(official_id),
    assignment      TEXT,                   -- 'Crew Chief', 'Referee', 'Umpire'
    PRIMARY KEY (game_id, official_id)
);

-- PLAYER GAME LOG (traditional box score)
CREATE TABLE player_game_log (
    log_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id             TEXT NOT NULL REFERENCES game(game_id),
    player_id           INTEGER NOT NULL REFERENCES player(player_id),
    team_id             INTEGER NOT NULL REFERENCES team(team_id),
    season_id           INTEGER NOT NULL REFERENCES season(season_id),
    start_position      TEXT,
    comment             TEXT,              -- 'DID NOT PLAY', 'INACTIVE', etc.
    minutes_played      REAL,
    fgm INTEGER, fga INTEGER, fg_pct REAL,
    fg3m INTEGER, fg3a INTEGER, fg3_pct REAL,
    ftm INTEGER, fta INTEGER, ft_pct REAL,
    oreb INTEGER, dreb INTEGER, reb INTEGER,
    ast INTEGER, stl INTEGER, blk INTEGER,
    tov INTEGER, pf INTEGER, pts INTEGER,
    plus_minus INTEGER,
    UNIQUE(game_id, player_id, team_id)
);

-- PLAYER GAME LOG ADVANCED
CREATE TABLE player_game_log_advanced (
    log_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id             TEXT NOT NULL REFERENCES game(game_id),
    player_id           INTEGER NOT NULL REFERENCES player(player_id),
    team_id             INTEGER NOT NULL REFERENCES team(team_id),
    minutes_played      REAL,
    off_rating          REAL, def_rating REAL, net_rating REAL,
    ast_pct             REAL, ast_to_tov REAL, ast_ratio REAL,
    oreb_pct            REAL, dreb_pct   REAL, reb_pct   REAL,
    tov_pct             REAL, efg_pct    REAL, ts_pct    REAL,
    usg_pct             REAL, pace       REAL, pie       REAL,
    UNIQUE(game_id, player_id, team_id)
);

-- PLAYER GAME LOG HUSTLE (2015–16+)
CREATE TABLE player_game_log_hustle (
    log_id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id                TEXT NOT NULL REFERENCES game(game_id),
    player_id              INTEGER NOT NULL REFERENCES player(player_id),
    team_id                INTEGER NOT NULL REFERENCES team(team_id),
    minutes_played         REAL,
    contested_shots        INTEGER, contested_shots_2pt INTEGER, contested_shots_3pt INTEGER,
    deflections            INTEGER, charges_drawn INTEGER,
    screen_assists         INTEGER, screen_ast_pts INTEGER,
    box_outs               INTEGER, off_box_outs INTEGER, def_box_outs INTEGER,
    loose_balls_recovered  INTEGER,
    UNIQUE(game_id, player_id, team_id)
);

-- TEAM GAME LOG (traditional)
CREATE TABLE team_game_log (
    log_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id         TEXT NOT NULL REFERENCES game(game_id),
    team_id         INTEGER NOT NULL REFERENCES team(team_id),
    season_id       INTEGER NOT NULL REFERENCES season(season_id),
    is_home         INTEGER NOT NULL,
    fgm INTEGER, fga INTEGER, fg_pct REAL,
    fg3m INTEGER, fg3a INTEGER, fg3_pct REAL,
    ftm INTEGER, fta INTEGER, ft_pct REAL,
    oreb INTEGER, dreb INTEGER, reb INTEGER,
    ast INTEGER, stl INTEGER, blk INTEGER,
    tov INTEGER, pf INTEGER, pts INTEGER,
    plus_minus INTEGER, pace REAL,
    UNIQUE(game_id, team_id)
);

-- PLAY BY PLAY
CREATE TABLE play_by_play (
    pbp_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id             TEXT NOT NULL REFERENCES game(game_id),
    event_num           INTEGER NOT NULL,
    period              INTEGER NOT NULL,
    pc_time             INTEGER,           -- period clock in seconds remaining
    wc_time             TEXT,              -- wall clock time (when available)
    event_type          INTEGER NOT NULL,  -- NBA.com event_msg_type (1-21)
    event_action_type   INTEGER,
    description_home    TEXT,
    description_visitor TEXT,
    score_home          INTEGER,
    score_visitor       INTEGER,
    score_margin        INTEGER,
    player1_id          INTEGER REFERENCES player(player_id),
    player1_team_id     INTEGER REFERENCES team(team_id),
    player2_id          INTEGER REFERENCES player(player_id),
    player2_team_id     INTEGER REFERENCES team(team_id),
    player3_id          INTEGER REFERENCES player(player_id),
    player3_team_id     INTEGER REFERENCES team(team_id),
    video_available     INTEGER NOT NULL DEFAULT 0,
    UNIQUE(game_id, event_num)
);

-- SHOT CHART
CREATE TABLE shot_chart (
    shot_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id             TEXT NOT NULL REFERENCES game(game_id),
    player_id           INTEGER NOT NULL REFERENCES player(player_id),
    team_id             INTEGER NOT NULL REFERENCES team(team_id),
    period              INTEGER NOT NULL,
    minutes_remaining   INTEGER,
    seconds_remaining   INTEGER,
    action_type         TEXT,              -- 'Jump Shot', 'Layup', etc.
    shot_type           TEXT,              -- '2PT Field Goal', '3PT Field Goal'
    shot_zone_basic     TEXT,
    shot_zone_area      TEXT,
    shot_zone_range     TEXT,
    shot_distance       INTEGER,           -- feet
    loc_x               INTEGER,           -- tenths of feet from basket
    loc_y               INTEGER,
    shot_made_flag      INTEGER NOT NULL,  -- 1 = made, 0 = missed
    htm                 TEXT,              -- home team during this possession
    vtm                 TEXT
);

-- DRAFT
CREATE TABLE draft (
    draft_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    draft_year      INTEGER NOT NULL,
    draft_round     INTEGER NOT NULL,
    draft_number    INTEGER NOT NULL,
    team_id         INTEGER REFERENCES team(team_id),
    player_id       INTEGER REFERENCES player(player_id),
    organization    TEXT,                 -- college or country of origin
    organization_type TEXT,              -- 'College', 'International', 'HS', etc.
    UNIQUE(draft_year, draft_round, draft_number)
);

-- AWARD
CREATE TABLE award (
    award_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id       INTEGER NOT NULL REFERENCES player(player_id),
    season_id       INTEGER NOT NULL REFERENCES season(season_id),
    award_type      TEXT NOT NULL,        -- 'MVP', 'ROY', 'DPOY', 'All-NBA', 'All-Defensive', 'All-Star', etc.
    award_tier      TEXT,                 -- '1st Team', '2nd Team', NULL for non-tiered awards (MVP, ROY, etc.)
    conference      TEXT,                 -- for conference-specific awards
    UNIQUE(player_id, season_id, award_type, award_tier)
    -- award_tier is included so that 'All-NBA 1st Team' and 'All-NBA 2nd Team' are treated
    -- as distinct constraint keys. Without it, award_type alone would incorrectly block
    -- inserting two tier-differentiated rows for the same player/season.
);

-- PLAYER SEASON STATS (aggregated)
CREATE TABLE player_season_stats (
    stat_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id       INTEGER NOT NULL REFERENCES player(player_id),
    team_id         INTEGER NOT NULL DEFAULT 0,
    -- team_id = 0 is a sentinel for all-teams aggregate rows (players traded mid-season,
    -- analogous to Basketball-Reference TOT rows). NULL is intentionally avoided:
    -- SQLite treats NULLs as distinct values in UNIQUE constraints, which would silently
    -- allow duplicate aggregate rows for the same player/season/stat_type combination.
    -- A sentinel team record (team_id = 0, team_name = 'TOT') must be inserted during DB init.
    season_id       INTEGER NOT NULL REFERENCES season(season_id),
    stat_type       TEXT NOT NULL,        -- 'Regular Season', 'Playoffs'
    games_played    INTEGER,
    games_started   INTEGER,
    minutes_played  REAL,
    fgm REAL, fga REAL, fg_pct REAL,
    fg3m REAL, fg3a REAL, fg3_pct REAL,
    ftm REAL, fta REAL, ft_pct REAL,
    oreb REAL, dreb REAL, reb REAL,
    ast REAL, stl REAL, blk REAL,
    tov REAL, pf REAL, pts REAL,
    -- Advanced
    off_rating REAL, def_rating REAL, net_rating REAL,
    ts_pct REAL, efg_pct REAL, usg_pct REAL,
    per REAL, ws REAL, bpm REAL, vorp REAL,
    UNIQUE(player_id, team_id, season_id, stat_type)
);

-- PLAYOFF SERIES
CREATE TABLE playoff_series (
    series_id       TEXT PRIMARY KEY,     -- e.g. '2024_E1_BOS_MIA'
    season_id       INTEGER NOT NULL REFERENCES season(season_id),
    round           TEXT NOT NULL,
    conference      TEXT,
    home_team_id    INTEGER NOT NULL REFERENCES team(team_id),
    away_team_id    INTEGER NOT NULL REFERENCES team(team_id),
    home_team_wins  INTEGER NOT NULL DEFAULT 0,
    away_team_wins  INTEGER NOT NULL DEFAULT 0,
    winner_team_id  INTEGER REFERENCES team(team_id),
    series_length   INTEGER
);

-- TRANSACTION
CREATE TABLE transaction (
    transaction_id  TEXT PRIMARY KEY,     -- UUID
    transaction_date TEXT NOT NULL,
    transaction_type TEXT NOT NULL,       -- 'Trade', 'Sign', 'Waive', 'Two-Way', 'Extension'
    player_id       INTEGER NOT NULL REFERENCES player(player_id),
    from_team_id    INTEGER REFERENCES team(team_id),
    to_team_id      INTEGER REFERENCES team(team_id),
    notes           TEXT,
    source          TEXT                  -- 'Basketball-Reference', 'RealGM'
);

-- EVENT MESSAGE TYPE LOOKUP
CREATE TABLE event_message_type (
    event_type_id   INTEGER PRIMARY KEY,
    event_name      TEXT NOT NULL         -- 'Field Goal Made', 'Turnover', etc.
);

-- PLAYER ID CROSS-REFERENCE
CREATE TABLE player_id_xref (
    player_id       INTEGER NOT NULL REFERENCES player(player_id),
    id_system       TEXT NOT NULL,        -- 'basketball_reference', 'aba_encyclopedia', 'realgm'
    external_id     TEXT NOT NULL,
    PRIMARY KEY (player_id, id_system)
);

-- DATA AVAILABILITY FLAG DEFINITIONS
-- Bit definitions for the data_availability_flags INTEGER column on game and player tables.
-- Query pattern: WHERE (data_availability_flags & <bit_value>) = <bit_value>
-- Example: all games with play-by-play → WHERE (data_availability_flags & 4) = 4
CREATE TABLE data_availability_flag_def (
    bit_position    INTEGER PRIMARY KEY,  -- 0-indexed bit position
    bit_value       INTEGER NOT NULL,     -- 2 ^ bit_position
    flag_name       TEXT NOT NULL UNIQUE,
    description     TEXT
);
-- Seed data (INSERT during DB initialization):
-- (0,   1,   'BOXSCORE_TRADITIONAL', 'Traditional team + player box score loaded')
-- (1,   2,   'BOXSCORE_ADVANCED',    'Advanced box score loaded')
-- (2,   4,   'PLAY_BY_PLAY',         'Play-by-play events loaded (~1996-97 onward)')
-- (3,   8,   'SHOT_CHART',           'Shot chart coordinates loaded (~1996-97 onward)')
-- (4,   16,  'OFFICIALS',            'Referee assignments loaded (~1990-91 onward)')
-- (5,   32,  'HUSTLE_STATS',         'Hustle stats loaded (2015-16 onward)')
-- (6,   64,  'TRACKING_STATS',       'Speed/distance tracking loaded (2013-14 onward)')

-- INGESTION AUDIT
CREATE TABLE ingestion_audit (
    audit_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type     TEXT NOT NULL,
    entity_id       TEXT NOT NULL,
    source          TEXT NOT NULL,
    ingest_ts       TEXT NOT NULL,        -- ISO datetime
    status          TEXT NOT NULL,        -- 'SUCCESS', 'EMPTY', 'FAILED', 'SKIPPED'
    row_count       INTEGER,
    error_message   TEXT,
    UNIQUE(entity_type, entity_id, source)
);

-- PERFORMANCE INDEXES
-- Required to meet the query latency targets in Section 11. Without these, full-history
-- analytical scans will exceed targets by 10-100x on a 40 GB database.
CREATE INDEX idx_player_game_log_season_player  ON player_game_log(season_id, player_id);
CREATE INDEX idx_player_game_log_game           ON player_game_log(game_id);
CREATE INDEX idx_player_game_log_adv_game       ON player_game_log_advanced(game_id, player_id);
CREATE INDEX idx_team_game_log_season           ON team_game_log(season_id);
CREATE INDEX idx_play_by_play_game_period       ON play_by_play(game_id, period);
CREATE INDEX idx_shot_chart_player_game         ON shot_chart(player_id, game_id);
CREATE INDEX idx_shot_chart_zone                ON shot_chart(shot_zone_basic, shot_zone_area);
CREATE INDEX idx_game_season_type               ON game(season_id, game_type);
CREATE INDEX idx_game_date                      ON game(game_date);
CREATE INDEX idx_player_season_stats_player     ON player_season_stats(player_id, season_id);
CREATE INDEX idx_player_season_stats_season     ON player_season_stats(season_id, stat_type);
CREATE INDEX idx_coach_stint_season             ON coach_stint(season_id);
CREATE INDEX idx_draft_year                     ON draft(draft_year);
CREATE INDEX idx_award_season                   ON award(season_id, award_type);
CREATE INDEX idx_ingestion_audit_entity         ON ingestion_audit(entity_type, status);
```

---

## 11. Performance Requirements

| Metric | Target | Measurement Method |
|---|---|---|
| Full historical backfill time | < 72 hours single-threaded; < 24 hours with `--workers 4` season-parallel mode (see loading strategy note below) | Timed pipeline run from clean state |
| Daily incremental update | < 10 minutes | Timed cron run day-after game night |
| SQLite point lookup by player_id | < 10 ms | `EXPLAIN QUERY PLAN` + timer |
| SQLite season box score scan (1 season) | < 200 ms | Timer on `SELECT * FROM player_game_log WHERE season_id = ?` |
| DuckDB full-history player aggregation | < 3 seconds | Timer on career totals view over all seasons |
| DuckDB shot chart zone aggregation (all time) | < 5 seconds | Timer on `GROUP BY shot_zone_basic` query |
| Database file size (SQLite, full history) | < 40 GB | `ls -lh nba.sqlite` |
| DuckDB file size (compressed) | < 15 GB | `ls -lh nba.duckdb` |
| API ingestion rate (sustained) | ≤ 8 req/min to stats.nba.com | Rate-limiter instrumentation log |

**Loading strategy:** Historical data is batched by season in reverse chronological order (most recent first) so the database is immediately useful for recent-era queries. Play-by-play and shot chart tables are loaded last (largest volume). DuckDB reads from SQLite via `sqlite_scan()` extension; no ETL duplication.

**Backfill time math:** ~7,300 games × 5 endpoints/game + ~5,000 player biography calls + ~200 metadata calls ≈ 41,700 API calls total. At ≤ 8 req/min sustained, that is approximately 87 hours single-threaded. To reach the < 24 hour target: (a) use batch game-finder endpoints where a single call returns all game IDs in a season, reducing per-season fan-out; (b) run `--workers 4` parallel workers each processing a disjoint season range with independent rate-limit budgets (4 × 8 = 32 effective req/min aggregate). Historical game fetches are idempotent via the raw JSON cache, making parallel workers safe. The `ingest --full` CLI command must accept `--workers N` (default 1).

**DuckDB view refresh:** DuckDB (as of v1.0) does not support automatic materialized view refresh — there is no `CREATE MATERIALIZED VIEW ... WITH REFRESH` syntax. Views in `nba.duckdb` are re-materialized explicitly by the incremental update pipeline using `CREATE OR REPLACE VIEW` for logical views, or `DELETE FROM + INSERT INTO` for pre-computed summary tables. The pipeline must trigger this step explicitly after every incremental run; views do not auto-refresh on query.

**Pipeline sequencing (lock contention):** DuckDB's `sqlite_scan()` acquires a read lock on the SQLite file for the duration of its scan. If this overlaps with an active SQLite write transaction (e.g., the APScheduler incremental ingest job), the writer will queue and may timeout. Required sequence: (1) complete all SQLite write transactions, (2) run `PRAGMA wal_checkpoint(FULL)` to flush the WAL file, (3) trigger DuckDB re-materialization. Never run these steps concurrently.

---

## 12. Known Risks & Trade-offs

| Risk | Likelihood | Impact | Mitigation / Accepted Trade-off |
|---|---|---|---|
| stats.nba.com changes API response schemas without notice | High | Medium | Pydantic validation with quarantine on failure; nba_api library community monitors and patches within days typically |
| stats.nba.com blocks or rate-limits the scraper IP | Medium | High | Back-off strategy + optional residential proxy support; community has operated nba_api for 7+ years at this scale |
| Pre-1973 data has fundamental completeness gaps (no play-by-play, limited box scores) | Certain | Low | `data_availability_flags` bitmask documents exactly what is present; views handle NULLs gracefully |
| Official/referee data only available from ~1990–91 | Certain | Low | `game_official` populated where available; NULL FK accepted for older games |
| ABA player ID cross-reference is manually curated and may have errors | Medium | Low | `player_id_xref` is versioned; community contributions can improve over time |
| DuckDB sqlite_scan() may have performance ceiling on 40 GB SQLite | Low | Medium | Fallback: export SQLite tables to Parquet and ingest natively into DuckDB; schema unchanged |
| Basketball-Reference ToS prohibits automated scraping | High | **High** | Basketball-Reference is the primary source for transactions and a key fallback for franchise history, coaching staff, draft history, biography enrichment, and awards. If BR blocks access: transactions → realgm.com (already listed as fallback in routing table); franchise history → `nba_api.FranchiseHistory`; coaching staff → `nba_api.CommonTeamRoster`; draft → `nba_api.DraftHistory`; biography enrichment → static community seed CSVs. Impact raised from Medium to **High** because several entity types have no other machine-readable alternative; all BR-dependent fallback paths must be implemented and integration-tested before V1 ship. Rate limit: 1 req/5 sec; users responsible for ToS compliance in their jurisdiction. |

**Sunk cost checkpoints:** If `swar/nba_api` becomes unmaintained or blocked within 12 months of this project launch, migrate primary ingestion to direct `httpx` calls against stats.nba.com with the same endpoint taxonomy; estimated 2-day migration effort. If DuckDB `sqlite_scan()` proves insufficient for query latency targets on full-history aggregations, export all core tables to Parquet and pivot the analytical layer to pure Parquet + DuckDB native storage; estimated 1-day migration effort.

---

## 13. Extensibility Contract

> How does someone add a new data domain without understanding the whole system?

- **Minimum surface to implement a new ingestion module:** Define a Python class inheriting from `BaseIngestor` implementing three methods: `fetch(entity_id: str) -> dict`, `validate(raw: dict) -> PydanticModel`, and `upsert(model: PydanticModel, conn: sqlite3.Connection) -> int`. Register the class in `ingestion/registry.py`. The scheduler discovers it automatically.
- **Minimum surface to add a DuckDB view:** Add a `.sql` file to `duckdb/views/` following the naming convention `v_{view_name}.sql`. The DuckDB build step auto-executes all files in that directory.
- **Schema migrations:** Add a timestamped file to `migrations/` using `yoyo-migrations` conventions. Running `nba-vault migrate` applies pending migrations in order.
- **Adding a new award type:** Insert a row into the `award` table with the appropriate `award_type` string. No schema change required.
- **Adding WNBA or G-League:** Add `league_id = 'WNBA'` to `league` table, add franchise and team records with the appropriate `league_id` FK. The schema is already league-agnostic. Requires new ingestor modules for WNBA-specific nba_api endpoints.
- **Contribution guidelines:** PR must include: migration file (if schema changes), updated Pydantic model, passing `pytest` unit tests for validation, and updated `data_dictionary.md` entry.

---

## 14. Open Questions

| Question | Owner | Due Date | Status |
|---|---|---|---|
| Does stats.nba.com have a canonical `franchise_history` endpoint that maps all historical relocations and renames? (FranchiseHistory endpoint exists but completeness unconfirmed for BAA era) | Nick | 2026-03-01 | Open |
| What is the earliest season for which `PlayByPlayV2` returns non-empty results? (Community reports suggest ~1996–97) | Nick | 2026-03-01 | Open |
| Is the Basketball-Reference player-ID-to-NBA.com-player-ID mapping available as an open dataset, or does it need to be constructed? | Nick | 2026-03-01 | Open |
| Should the `player_season_stats` table store per-game averages, totals, or both? (Currently both via separate `stat_type` rows — confirm this is acceptable column explosion vs. normalization) | Nick | 2026-03-01 | Open |
| What license should the database itself be released under given that source data is from stats.nba.com? | Nick | 2026-03-15 | Open |
| Confirm DuckDB 1.0 `sqlite_scan()` stability on > 20 GB SQLite files — may require benchmark before committing to architecture | Nick | 2026-03-01 | Open |

---

## 15. Out-of-Scope Deferral Log

| Idea | Why Deferred | Revisit Trigger |
|---|---|---|
| WNBA and G-League data | Schema supports it but would double ingestion complexity and scope for V0 | V1 milestone or community contributor |
| Salary / contract data | Legal ambiguity; no clear open-source data source; Spotrac/HoopsHype prohibit scraping | Dedicated contributor with legal clarity |
| Real-time game streaming | Requires websocket infrastructure; out of scope for local-first analytical tool | If REST API wrapper is built in V1 |
| Second Spectrum player tracking (XY coordinates per possession) | Proprietary; no legal open-source access | If NBA releases tracking data publicly |
| Predictive / ML feature store | Consumer of this database, not part of it | Separate repository consuming this as dependency |
| REST API / Datasette web UI | Nice for exploration but adds operational surface area | V1 milestone; Datasette integration is 1-day effort |
| Historical media (game video, photos) | Storage prohibitive; rights unclear | Out of scope indefinitely |
| Referee bias / foul tendency analytics | Can be computed from existing schema; belongs in consumer analytics layer | Analytics library built on top of this DB |

---

## 16. Success Criteria

> How do you know when this project is done enough to ship as V1?

- [ ] `nba-vault ingest --full` completes without fatal errors and produces a valid `nba.sqlite` file
- [ ] All 78 NBA/ABA/BAA seasons are represented in the `season` table
- [ ] All 30 current NBA franchises plus historical franchises (Sonics, Nets in NJ, etc.) are in `franchise` with correct `franchise_id` continuity
- [ ] Player game logs are populated for ≥ 95% of all games in the database
- [ ] Head coach records are populated for all 78 seasons across all franchises
- [ ] Draft history is complete back to 1947
- [ ] Award records (MVP, All-NBA, All-Star) are complete back to first availability
- [ ] DuckDB analytical database builds without error and all 5 core views return correct results on spot-check queries
- [ ] `nba-vault ingest --incremental` correctly identifies and ingests the previous night's completed games
- [ ] `pytest` test suite passes with ≥ 85% coverage on ingestion, validation, and upsert modules
- [ ] `data_dictionary.md` documents every table and column
- [ ] README includes quickstart (install → full ingest → first query) completing in documented time on reference hardware

---

*Last updated: 2026-02-17 by Nick*
```
