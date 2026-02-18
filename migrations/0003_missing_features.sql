-- Missing Features Migration
-- Based on comprehensive schema audit
-- Adds tables and fields identified as missing from NBA data sources

-- ========================================
-- MISSING TABLES
-- ========================================

-- BOX SCORE OTHER STATS (Game-Level Team Metrics)
CREATE TABLE IF NOT EXISTS team_game_other_stats (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL REFERENCES game(game_id),
    team_id INTEGER NOT NULL REFERENCES team(team_id),
    season_id INTEGER NOT NULL REFERENCES season(season_id),
    points_paint INTEGER,
    points_second_chance INTEGER,
    points_fast_break INTEGER,
    largest_lead INTEGER,
    lead_changes INTEGER,
    times_tied INTEGER,
    team_turnovers INTEGER,
    total_turnovers INTEGER,
    team_rebounds INTEGER,
    points_off_turnovers INTEGER,
    UNIQUE(game_id, team_id)
);

-- PLAYER TRACKING STATS (Speed & Distance - 2013-14+)
CREATE TABLE IF NOT EXISTS player_game_tracking (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL REFERENCES game(game_id),
    player_id INTEGER NOT NULL REFERENCES player(player_id),
    team_id INTEGER NOT NULL REFERENCES team(team_id),
    season_id INTEGER NOT NULL REFERENCES season(season_id),
    minutes_played REAL,
    distance_miles REAL,
    distance_miles_offensive REAL,
    distance_miles_defensive REAL,
    speed_mph_avg REAL,
    speed_mph_max REAL,
    touches INTEGER,
    touches_catch_shoot INTEGER,
    touches_paint INTEGER,
    touches_post_up INTEGER,
    drives INTEGER,
    drives_pts INTEGER,
    pull_up_shots INTEGER,
    pull_up_shots_made INTEGER,
    UNIQUE(game_id, player_id, team_id)
);

-- LINEUP DATA
CREATE TABLE IF NOT EXISTS lineup (
    lineup_id TEXT PRIMARY KEY,
    season_id INTEGER NOT NULL REFERENCES season(season_id),
    team_id INTEGER NOT NULL REFERENCES team(team_id),
    player_1_id INTEGER NOT NULL REFERENCES player(player_id),
    player_2_id INTEGER NOT NULL REFERENCES player(player_id),
    player_3_id INTEGER NOT NULL REFERENCES player(player_id),
    player_4_id INTEGER NOT NULL REFERENCES player(player_id),
    player_5_id INTEGER NOT NULL REFERENCES player(player_id),
    minutes_played REAL DEFAULT 0,
    possessions INTEGER DEFAULT 0,
    points_scored INTEGER DEFAULT 0,
    points_allowed INTEGER DEFAULT 0,
    off_rating REAL,
    def_rating REAL,
    net_rating REAL,
    CONSTRAINT unique_lineup UNIQUE(player_1_id, player_2_id, player_3_id, player_4_id, player_5_id, team_id, season_id)
);

CREATE TABLE IF NOT EXISTS lineup_game_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    lineup_id TEXT NOT NULL REFERENCES lineup(lineup_id),
    game_id TEXT NOT NULL REFERENCES game(game_id),
    team_id INTEGER NOT NULL REFERENCES team(team_id),
    minutes_played REAL,
    plus_minus INTEGER,
    possessions INTEGER,
    points_scored INTEGER,
    points_allowed INTEGER,
    UNIQUE(lineup_id, game_id)
);

-- POSSESSION-LEVEL DATA
CREATE TABLE IF NOT EXISTS possession (
    possession_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL REFERENCES game(game_id),
    possession_number INTEGER NOT NULL,
    period INTEGER NOT NULL,
    start_time REAL NOT NULL,
    end_time REAL,
    team_id INTEGER NOT NULL REFERENCES team(team_id),
    points_scored INTEGER DEFAULT 0,
    play_type TEXT,
    shot_clock_start REAL,
    shot_clock_end REAL,
    duration_seconds REAL,
    outcome_type TEXT,
    UNIQUE(game_id, possession_number)
);

-- INJURY DATA
CREATE TABLE IF NOT EXISTS injury (
    injury_id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL REFERENCES player(player_id),
    team_id INTEGER REFERENCES team(team_id),
    injury_date TEXT NOT NULL,
    injury_type TEXT,
    body_part TEXT,
    status TEXT,
    games_missed INTEGER DEFAULT 0,
    return_date TEXT,
    notes TEXT
);

-- PLAYER CONTRACT/SALARY DATA
CREATE TABLE IF NOT EXISTS player_contract (
    contract_id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL REFERENCES player(player_id),
    team_id INTEGER NOT NULL REFERENCES team(team_id),
    season_start INTEGER NOT NULL,
    season_end INTEGER NOT NULL,
    salary_amount REAL,
    contract_type TEXT,
    player_option INTEGER DEFAULT 0,
    team_option INTEGER DEFAULT 0,
    early_termination INTEGER DEFAULT 0,
    guaranteed_money REAL,
    cap_hit REAL,
    dead_money REAL,
    UNIQUE(player_id, team_id, season_start)
);

-- DRAFT COMBINE DATA
CREATE TABLE IF NOT EXISTS draft_combine (
    combine_id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL REFERENCES player(player_id),
    draft_year INTEGER NOT NULL,
    height_wo_shoes_inches REAL,
    height_w_shoes_inches REAL,
    weight_lbs REAL,
    wingspan_inches REAL,
    standing_reach_inches REAL,
    body_fat_pct REAL,
    hand_length_inches REAL,
    hand_width_inches REAL,
    bench_press_reps INTEGER,
    vertical_leap_standing_inches REAL,
    vertical_leap_max_inches REAL,
    lane_agility_time_sec REAL,
    three_quarter_sprint_sec REAL,
    UNIQUE(player_id, draft_year)
);

-- PLAYER GAME MISC STATS
CREATE TABLE IF NOT EXISTS player_game_misc_stats (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL REFERENCES game(game_id),
    player_id INTEGER NOT NULL REFERENCES player(player_id),
    team_id INTEGER NOT NULL REFERENCES team(team_id),
    season_id INTEGER NOT NULL REFERENCES season(season_id),
    plus_minus INTEGER,
    double_double INTEGER DEFAULT 0,
    triple_double INTEGER DEFAULT 0,
    quadruple_double INTEGER DEFAULT 0,
    five_by_five INTEGER DEFAULT 0,
    points_generated INTEGER,
    game_score REAL,
    efficiency REAL,
    UNIQUE(game_id, player_id, team_id)
);

-- PLAYER SEASON METADATA (for enhanced tracking)
CREATE TABLE IF NOT EXISTS player_season_metadata (
    metadata_id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL REFERENCES player(player_id),
    season_id INTEGER NOT NULL REFERENCES season(season_id),
    team_id INTEGER NOT NULL REFERENCES team(team_id),
    age INTEGER,
    games_started INTEGER,
    minutes_per_game REAL,
    experience_years INTEGER,
    salary REAL,
    cap_hit REAL,
    contract_year INTEGER DEFAULT 0,
    UNIQUE(player_id, season_id, team_id)
);

-- TEAM SEASON ADVANCED STATS
CREATE TABLE IF NOT EXISTS team_season_advanced (
    stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
    team_id INTEGER NOT NULL REFERENCES team(team_id),
    season_id INTEGER NOT NULL REFERENCES season(season_id),
    off_rating REAL,
    def_rating REAL,
    net_rating REAL,
    pace REAL,
    effective_fg_pct REAL,
    turnover_pct REAL,
    offensive_rebound_pct REAL,
    free_throw_rate REAL,
    three_point_rate REAL,
    true_shooting_pct REAL,
    UNIQUE(team_id, season_id)
);

-- ========================================
-- ADD MISSING COLUMNS TO EXISTING TABLES
-- ========================================

-- PLAYER GAME LOG ADVANCED - Missing Fields
ALTER TABLE player_game_log_advanced ADD COLUMN tm_tov_pct REAL;
ALTER TABLE player_game_log_advanced ADD COLUMN usg_pct_precise REAL;

-- PLAYER SEASON STATS - Missing Advanced Metrics
ALTER TABLE player_season_stats ADD COLUMN ows REAL;
ALTER TABLE player_season_stats ADD COLUMN dws REAL;
ALTER TABLE player_season_stats ADD COLUMN obpm REAL;
ALTER TABLE player_season_stats ADD COLUMN dbpm REAL;
ALTER TABLE player_season_stats ADD COLUMN game_score_avg REAL;

-- PLAYER SEASON STATS - Additional Basketball Reference Metrics
ALTER TABLE player_season_stats ADD COLUMN three_point_attempt_rate REAL;
ALTER TABLE player_season_stats ADD COLUMN free_throw_rate REAL;
ALTER TABLE player_season_stats ADD COLUMN ws_per_48 REAL;

-- TEAM GAME LOG - Missing Advanced Team Metrics
ALTER TABLE team_game_log ADD COLUMN off_rating REAL;
ALTER TABLE team_game_log ADD COLUMN def_rating REAL;
ALTER TABLE team_game_log ADD COLUMN net_rating REAL;
ALTER TABLE team_game_log ADD COLUMN effective_fg_pct REAL;
ALTER TABLE team_game_log ADD COLUMN turnover_pct REAL;
ALTER TABLE team_game_log ADD COLUMN offensive_rebound_pct REAL;
ALTER TABLE team_game_log ADD COLUMN free_throw_rate REAL;

-- PLAY BY PLAY - Enhanced Fields
ALTER TABLE play_by_play ADD COLUMN possession_number INTEGER;
ALTER TABLE play_by_play ADD COLUMN shot_distance_feet INTEGER;
ALTER TABLE play_by_play ADD COLUMN shot_clock_remaining REAL;
ALTER TABLE play_by_play ADD COLUMN points_scored INTEGER;
ALTER TABLE play_by_play ADD COLUMN challenge_flag INTEGER DEFAULT 0;
ALTER TABLE play_by_play ADD COLUMN review_type TEXT;

-- SHOT CHART - Missing Shot Quality Fields
ALTER TABLE shot_chart ADD COLUMN shot_quality_grade TEXT;
ALTER TABLE shot_chart ADD COLUMN defender_distance INTEGER;
ALTER TABLE shot_chart ADD COLUMN shot_pressure TEXT;
ALTER TABLE shot_chart ADD COLUMN shot_clock_time REAL;
ALTER TABLE shot_chart ADD COLUMN dribble_count INTEGER;
ALTER TABLE shot_chart ADD COLUMN touch_time REAL;
ALTER TABLE shot_chart ADD COLUMN creation_type TEXT;
ALTER TABLE shot_chart ADD COLUMN shot_result_detailed TEXT;

-- PLAYER GAME LOG HUSTLE - Missing Fields
ALTER TABLE player_game_log_hustle ADD COLUMN box_outs_won INTEGER;
ALTER TABLE player_game_log_hustle ADD COLUMN box_outs_total INTEGER;
ALTER TABLE player_game_log_hustle ADD COLUMN box_outs_won_pct REAL;
ALTER TABLE player_game_log_hustle ADD COLUMN charges_drawn_separate INTEGER;
ALTER TABLE player_game_log_hustle ADD COLUMN screen_assists_points INTEGER;

-- AWARD - Enhanced Fields
ALTER TABLE award ADD COLUMN vote_points INTEGER;
ALTER TABLE award ADD COLUMN first_place_votes INTEGER;
ALTER TABLE award ADD COLUMN award_rank INTEGER;
ALTER TABLE award ADD COLUMN voting_share_pct REAL;

-- TRANSACTION - Enhanced Fields
ALTER TABLE "transaction" ADD COLUMN trade_details TEXT;
ALTER TABLE "transaction" ADD COLUMN players_involved TEXT;
ALTER TABLE "transaction" ADD COLUMN draft_picks_involved TEXT;
ALTER TABLE "transaction" ADD COLUMN cash_considerations REAL;

-- PLAYER - Enhanced Fields
ALTER TABLE player ADD COLUMN high_school TEXT;
ALTER TABLE player ADD COLUMN draft_team_id INTEGER REFERENCES team(team_id);
ALTER TABLE player ADD COLUMN international_country TEXT;
ALTER TABLE player ADD COLUMN nba_debut_age REAL;

-- GAME - Enhanced Fields
ALTER TABLE game ADD COLUMN home_rest_days INTEGER;
ALTER TABLE game ADD COLUMN away_rest_days INTEGER;
ALTER TABLE game ADD COLUMN home_back_to_back INTEGER DEFAULT 0;
ALTER TABLE game ADD COLUMN away_back_to_back INTEGER DEFAULT 0;
ALTER TABLE game ADD COLUMN travel_distance_home INTEGER;
ALTER TABLE game ADD COLUMN travel_distance_away INTEGER;
ALTER TABLE game ADD COLUMN altitude_diff INTEGER;

-- ========================================
-- ADD NEW DATA AVAILABILITY FLAGS
-- ========================================

INSERT INTO data_availability_flag_def (bit_position, bit_value, flag_name, description) VALUES
    (7, 128, 'LINEUP_DATA', 'Lineup combination data loaded'),
    (8, 256, 'POSSESSION_DATA', 'Possession-level tracking loaded'),
    (9, 512, 'PLAYER_TRACKING', 'Player movement/speed data loaded (2013-14+)'),
    (10, 1024, 'INJURY_DATA', 'Injury status data loaded'),
    (11, 2048, 'SALARY_DATA', 'Contract and salary data loaded'),
    (12, 4096, 'THREE_POINT_DATA', '3-point shooting data loaded (1979-80+)'),
    (13, 8192, 'TURNOVER_DATA', 'Turnover data loaded (1977-78+)'),
    (14, 16384, 'OTHER_STATS', 'Box score other stats (paint points, etc.) loaded'),
    (15, 32768, 'ADVANCED_TEAM_STATS', 'Advanced team statistics loaded')
ON CONFLICT (flag_name) DO UPDATE SET description = excluded.description;
