-- ELO and RAPTOR Tables Migration
-- Adds game_elo (FiveThirtyEight ELO ratings, 1946-present) and
-- player_raptor (FiveThirtyEight RAPTOR metrics, 1976-present)

-- GAME ELO
-- One row per team per game. Source: Neil-Paine-1/NBA-elo (MIT) and
-- fivethirtyeight/data nba-elo (CC BY 4.0).
-- game_id is NULL for pre-NBA.com era games (pre-1996) where no NBA.com game_id exists.
CREATE TABLE IF NOT EXISTS game_elo (
    elo_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id       TEXT REFERENCES game(game_id),  -- NULL for pre-NBA.com era
    game_date     TEXT NOT NULL,                  -- ISO date YYYY-MM-DD
    season_id     INTEGER REFERENCES season(season_id),
    team_id       INTEGER REFERENCES team(team_id),
    bbref_team_id TEXT,                           -- Basketball-Reference team abbreviation
    elo_before    REAL NOT NULL,                  -- elo_i: ELO entering the game
    elo_after     REAL NOT NULL,                  -- elo_n: ELO following the game
    win_prob      REAL,                           -- forecast: pre-game ELO win probability
    win_equiv     REAL,                           -- equivalent wins in 82-game season
    opponent_elo  REAL,                           -- opp_elo_i
    game_location TEXT,                           -- 'H', 'A', 'N'
    pts_scored    INTEGER,
    pts_allowed   INTEGER,
    game_result   TEXT,                           -- 'W' or 'L'
    is_playoffs   INTEGER NOT NULL DEFAULT 0,
    notes         TEXT,
    source        TEXT NOT NULL DEFAULT 'fivethirtyeight',
    UNIQUE(game_date, bbref_team_id)
);

-- PLAYER RAPTOR
-- Per-player per-season RAPTOR ratings. Source: fivethirtyeight/nba-player-advanced-metrics (CC BY 4.0).
-- raptor_version distinguishes the three eras of RAPTOR calculation:
--   'modern'  (2014+): full player tracking + on/off + box
--   'mixed'   (2001-2013): box + single-year regularized plus-minus
--   'box'     (1977-2000): box score estimate only
CREATE TABLE IF NOT EXISTS player_raptor (
    raptor_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id            INTEGER REFERENCES player(player_id),
    bbref_id             TEXT,                    -- Basketball-Reference player slug
    season_id            INTEGER REFERENCES season(season_id),  -- NULL if season not yet loaded
    season_type          TEXT NOT NULL,           -- 'RS' (regular season) or 'PO' (playoffs)
    team_id              INTEGER REFERENCES team(team_id),
    bbref_team_id        TEXT,                    -- Basketball-Reference team abbreviation
    poss                 INTEGER,                 -- possessions played
    mp                   INTEGER,                 -- minutes played
    raptor_box_offense   REAL,                    -- box score offensive component
    raptor_box_defense   REAL,                    -- box score defensive component
    raptor_box_total     REAL,
    raptor_onoff_offense REAL,                    -- on/off offensive component
    raptor_onoff_defense REAL,
    raptor_onoff_total   REAL,
    raptor_offense       REAL,                    -- combined offensive RAPTOR
    raptor_defense       REAL,                    -- combined defensive RAPTOR
    raptor_total         REAL,                    -- total RAPTOR (points above avg per 100 poss)
    war_total            REAL,                    -- Wins Above Replacement (reg + playoffs)
    war_reg_season       REAL,
    war_playoffs         REAL,
    predator_offense     REAL,                    -- predictive RAPTOR offense
    predator_defense     REAL,
    predator_total       REAL,
    pace_impact          REAL,                    -- player impact on team possessions per 48 min
    raptor_version       TEXT,                    -- 'modern', 'mixed', 'box'
    UNIQUE(bbref_id, season_id, season_type, bbref_team_id)
);

-- INDEXES
CREATE INDEX IF NOT EXISTS idx_game_elo_game ON game_elo(game_id);
CREATE INDEX IF NOT EXISTS idx_game_elo_date ON game_elo(game_date);
CREATE INDEX IF NOT EXISTS idx_game_elo_season ON game_elo(season_id);
CREATE INDEX IF NOT EXISTS idx_game_elo_team ON game_elo(team_id);
CREATE INDEX IF NOT EXISTS idx_game_elo_bbref_team ON game_elo(bbref_team_id);
CREATE INDEX IF NOT EXISTS idx_game_elo_result ON game_elo(game_result, is_playoffs);

CREATE INDEX IF NOT EXISTS idx_player_raptor_player ON player_raptor(player_id);
CREATE INDEX IF NOT EXISTS idx_player_raptor_bbref ON player_raptor(bbref_id);
CREATE INDEX IF NOT EXISTS idx_player_raptor_season ON player_raptor(season_id, season_type);
CREATE INDEX IF NOT EXISTS idx_player_raptor_team ON player_raptor(team_id);
CREATE INDEX IF NOT EXISTS idx_player_raptor_total ON player_raptor(raptor_total);
CREATE INDEX IF NOT EXISTS idx_player_raptor_war ON player_raptor(war_total);
CREATE INDEX IF NOT EXISTS idx_player_raptor_version ON player_raptor(raptor_version, season_id);
