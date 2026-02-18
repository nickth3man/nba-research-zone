-- Initial NBA Vault database schema
-- This migration creates all core tables for the NBA database

-- RECOMMENDED SQLITE PRAGMA CONFIGURATION
-- These are applied in connection.py but documented here for reference
-- PRAGMA page_size = 16384;    -- 16 KB pages
-- PRAGMA journal_mode = WAL;   -- Write-ahead logging
-- PRAGMA synchronous = NORMAL; -- Adequate durability with WAL
-- PRAGMA foreign_keys = ON;    -- Enforce FK integrity
-- PRAGMA cache_size = -131072; -- 128 MB page cache

-- LEAGUE
CREATE TABLE IF NOT EXISTS league (
    league_id   TEXT PRIMARY KEY,  -- 'NBA', 'ABA', 'BAA'
    league_name TEXT NOT NULL,
    founded_year INTEGER,
    folded_year  INTEGER           -- NULL if active
);

-- SEASON
CREATE TABLE IF NOT EXISTS season (
    season_id       INTEGER PRIMARY KEY,  -- start year e.g. 2024
    league_id       TEXT NOT NULL REFERENCES league(league_id),
    season_label    TEXT NOT NULL,        -- '2024-25'
    games_per_team  INTEGER,
    schedule_start  TEXT,                 -- ISO date
    schedule_end    TEXT,
    champion_franchise_id INTEGER,
    finals_mvp_player_id  INTEGER
);

-- FRANCHISE
CREATE TABLE IF NOT EXISTS franchise (
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
CREATE TABLE IF NOT EXISTS team (
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
CREATE TABLE IF NOT EXISTS player (
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
CREATE TABLE IF NOT EXISTS coach (
    coach_id        INTEGER PRIMARY KEY,  -- NBA.com coach_id
    first_name      TEXT NOT NULL,
    last_name       TEXT NOT NULL,
    full_name       TEXT NOT NULL,
    birthdate       TEXT,
    college         TEXT,
    is_active       INTEGER NOT NULL DEFAULT 1
);

-- COACH STINT (per-team per-season assignment)
CREATE TABLE IF NOT EXISTS coach_stint (
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
CREATE TABLE IF NOT EXISTS official (
    official_id     INTEGER PRIMARY KEY,  -- NBA.com official_id
    first_name      TEXT NOT NULL,
    last_name       TEXT NOT NULL,
    full_name       TEXT NOT NULL,
    jersey_num      TEXT,
    is_active       INTEGER NOT NULL DEFAULT 1
);

-- ARENA
CREATE TABLE IF NOT EXISTS arena (
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
CREATE TABLE IF NOT EXISTS game (
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
CREATE TABLE IF NOT EXISTS game_official (
    game_id         TEXT NOT NULL REFERENCES game(game_id),
    official_id     INTEGER NOT NULL REFERENCES official(official_id),
    assignment      TEXT,                   -- 'Crew Chief', 'Referee', 'Umpire'
    PRIMARY KEY (game_id, official_id)
);

-- PLAYER GAME LOG (traditional box score)
CREATE TABLE IF NOT EXISTS player_game_log (
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
CREATE TABLE IF NOT EXISTS player_game_log_advanced (
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
CREATE TABLE IF NOT EXISTS player_game_log_hustle (
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
CREATE TABLE IF NOT EXISTS team_game_log (
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
CREATE TABLE IF NOT EXISTS play_by_play (
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
CREATE TABLE IF NOT EXISTS shot_chart (
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
CREATE TABLE IF NOT EXISTS draft (
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
CREATE TABLE IF NOT EXISTS award (
    award_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id       INTEGER NOT NULL REFERENCES player(player_id),
    season_id       INTEGER NOT NULL REFERENCES season(season_id),
    award_type      TEXT NOT NULL,        -- 'MVP', 'ROY', 'DPOY', 'All-NBA', 'All-Defensive', 'All-Star', etc.
    award_tier      TEXT,                 -- '1st Team', '2nd Team', NULL for non-tiered awards (MVP, ROY, etc.)
    conference      TEXT,                 -- for conference-specific awards
    UNIQUE(player_id, season_id, award_type, award_tier)
);

-- PLAYER SEASON STATS (aggregated)
CREATE TABLE IF NOT EXISTS player_season_stats (
    stat_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id       INTEGER NOT NULL REFERENCES player(player_id),
    team_id         INTEGER NOT NULL DEFAULT 0,
    -- team_id = 0 is a sentinel for all-teams aggregate rows (players traded mid-season,
    -- analogous to Basketball-Reference TOT rows). NULL is intentionally avoided.
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
CREATE TABLE IF NOT EXISTS playoff_series (
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
CREATE TABLE IF NOT EXISTS transaction (
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
CREATE TABLE IF NOT EXISTS event_message_type (
    event_type_id   INTEGER PRIMARY KEY,
    event_name      TEXT NOT NULL         -- 'Field Goal Made', 'Turnover', etc.
);

-- PLAYER ID CROSS-REFERENCE
CREATE TABLE IF NOT EXISTS player_id_xref (
    player_id       INTEGER NOT NULL REFERENCES player(player_id),
    id_system       TEXT NOT NULL,        -- 'basketball_reference', 'aba_encyclopedia', 'realgm'
    external_id     TEXT NOT NULL,
    PRIMARY KEY (player_id, id_system)
);

-- DATA AVAILABILITY FLAG DEFINITIONS
CREATE TABLE IF NOT EXISTS data_availability_flag_def (
    bit_position    INTEGER PRIMARY KEY,  -- 0-indexed bit position
    bit_value       INTEGER NOT NULL,     -- 2 ^ bit_position
    flag_name       TEXT NOT NULL UNIQUE,
    description     TEXT
);

-- INGESTION AUDIT
CREATE TABLE IF NOT EXISTS ingestion_audit (
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
CREATE INDEX IF NOT EXISTS idx_player_game_log_season_player ON player_game_log(season_id, player_id);
CREATE INDEX IF NOT EXISTS idx_player_game_log_game ON player_game_log(game_id);
CREATE INDEX IF NOT EXISTS idx_player_game_log_adv_game ON player_game_log_advanced(game_id, player_id);
CREATE INDEX IF NOT EXISTS idx_team_game_log_season ON team_game_log(season_id);
CREATE INDEX IF NOT EXISTS idx_play_by_play_game_period ON play_by_play(game_id, period);
CREATE INDEX IF NOT EXISTS idx_shot_chart_player_game ON shot_chart(player_id, game_id);
CREATE INDEX IF NOT EXISTS idx_shot_chart_zone ON shot_chart(shot_zone_basic, shot_zone_area);
CREATE INDEX IF NOT EXISTS idx_game_season_type ON game(season_id, game_type);
CREATE INDEX IF NOT EXISTS idx_game_date ON game(game_date);
CREATE INDEX IF NOT EXISTS idx_player_season_stats_player ON player_season_stats(player_id, season_id);
CREATE INDEX IF NOT EXISTS idx_player_season_stats_season ON player_season_stats(season_id, stat_type);
CREATE INDEX IF NOT EXISTS idx_coach_stint_season ON coach_stint(season_id);
CREATE INDEX IF NOT EXISTS idx_draft_year ON draft(draft_year);
CREATE INDEX IF NOT EXISTS idx_award_season ON award(season_id, award_type);
CREATE INDEX IF NOT EXISTS idx_ingestion_audit_entity ON ingestion_audit(entity_type, status);
