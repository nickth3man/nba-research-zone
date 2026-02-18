-- Performance Indexes for Missing Features Migration
-- Creates indexes for all new tables and columns added in migration 0003

-- ========================================
-- NEW TABLE INDEXES
-- ========================================

-- Team Game Other Stats
CREATE INDEX IF NOT EXISTS idx_team_game_other_stats_game ON team_game_other_stats(game_id);
CREATE INDEX IF NOT EXISTS idx_team_game_other_stats_season ON team_game_other_stats(season_id);
CREATE INDEX IF NOT EXISTS idx_team_game_other_stats_team ON team_game_other_stats(team_id);

-- Player Game Tracking
CREATE INDEX IF NOT EXISTS idx_player_game_tracking_game ON player_game_tracking(game_id);
CREATE INDEX IF NOT EXISTS idx_player_game_tracking_season_player ON player_game_tracking(season_id, player_id);
CREATE INDEX IF NOT EXISTS idx_player_game_tracking_team ON player_game_tracking(team_id);

-- Lineup
CREATE INDEX IF NOT EXISTS idx_lineup_season ON lineup(season_id);
CREATE INDEX IF NOT EXISTS idx_lineup_team ON lineup(team_id);
CREATE INDEX IF NOT EXISTS idx_lineup_players ON lineup(player_1_id, player_2_id, player_3_id, player_4_id, player_5_id);
CREATE INDEX IF NOT EXISTS idx_lineup_minutes ON lineup(minutes_played);

-- Lineup Game Log
CREATE INDEX IF NOT EXISTS idx_lineup_game_log_lineup ON lineup_game_log(lineup_id);
CREATE INDEX IF NOT EXISTS idx_lineup_game_log_game ON lineup_game_log(game_id);
CREATE INDEX IF NOT EXISTS idx_lineup_game_log_team ON lineup_game_log(team_id);

-- Possession
CREATE INDEX IF NOT EXISTS idx_possession_game ON possession(game_id);
CREATE INDEX IF NOT EXISTS idx_possession_game_period ON possession(game_id, period);
CREATE INDEX IF NOT EXISTS idx_possession_team ON possession(team_id);
CREATE INDEX IF NOT EXISTS idx_possession_outcome ON possession(outcome_type);
CREATE INDEX IF NOT EXISTS idx_possession_play_type ON possession(play_type);

-- Injury
CREATE INDEX IF NOT EXISTS idx_injury_player ON injury(player_id);
CREATE INDEX IF NOT EXISTS idx_injury_team ON injury(team_id);
CREATE INDEX IF NOT EXISTS idx_injury_date ON injury(injury_date);
CREATE INDEX IF NOT EXISTS idx_injury_status ON injury(status);

-- Player Contract
CREATE INDEX IF NOT EXISTS idx_player_contract_player ON player_contract(player_id);
CREATE INDEX IF NOT EXISTS idx_player_contract_team ON player_contract(team_id);
CREATE INDEX IF NOT EXISTS idx_player_contract_season ON player_contract(season_start, season_end);
CREATE INDEX IF NOT EXISTS idx_player_contract_salary ON player_contract(salary_amount);

-- Draft Combine
CREATE INDEX IF NOT EXISTS idx_draft_combine_player ON draft_combine(player_id);
CREATE INDEX IF NOT EXISTS idx_draft_combine_year ON draft_combine(draft_year);

-- Player Game Misc Stats
CREATE INDEX IF NOT EXISTS idx_player_game_misc_stats_game ON player_game_misc_stats(game_id);
CREATE INDEX IF NOT EXISTS idx_player_game_misc_stats_season_player ON player_game_misc_stats(season_id, player_id);
CREATE INDEX IF NOT EXISTS idx_player_game_misc_stats_team ON player_game_misc_stats(team_id);
CREATE INDEX IF NOT EXISTS idx_player_game_misc_stats_double_double ON player_game_misc_stats(double_double);
CREATE INDEX IF NOT EXISTS idx_player_game_misc_stats_triple_double ON player_game_misc_stats(triple_double);

-- Player Season Metadata
CREATE INDEX IF NOT EXISTS idx_player_season_metadata_player ON player_season_metadata(player_id);
CREATE INDEX IF NOT EXISTS idx_player_season_metadata_season ON player_season_metadata(season_id);
CREATE INDEX IF NOT EXISTS idx_player_season_metadata_team ON player_season_metadata(team_id);

-- Team Season Advanced
CREATE INDEX IF NOT EXISTS idx_team_season_advanced_team ON team_season_advanced(team_id);
CREATE INDEX IF NOT EXISTS idx_team_season_advanced_season ON team_season_advanced(season_id);
CREATE INDEX IF NOT EXISTS idx_team_season_advanced_off_rating ON team_season_advanced(off_rating);
CREATE INDEX IF NOT EXISTS idx_team_season_advanced_net_rating ON team_season_advanced(net_rating);

-- ========================================
-- COMPOUND INDEXES FOR COMMON QUERIES
-- ========================================

-- Lineup Performance
CREATE INDEX IF NOT EXISTS idx_lineup_performance ON lineup(net_rating, minutes_played);

-- Player Tracking Performance
CREATE INDEX IF NOT EXISTS idx_player_tracking_distance ON player_game_tracking(distance_miles, minutes_played);

-- Possession Analysis
CREATE INDEX IF NOT EXISTS idx_possession_analysis ON possession(game_id, team_id, outcome_type);

-- Injury Status
CREATE INDEX IF NOT EXISTS idx_injury_active ON injury(player_id, status) WHERE status = 'Active';

-- Contract Analysis
CREATE INDEX IF NOT EXISTS idx_contract_analysis ON player_contract(season_start, salary_amount);

-- ========================================
-- COVERING INDEXES FOR CRITICAL QUERIES
-- ========================================

-- Lineup Game Coverage
CREATE INDEX IF NOT EXISTS idx_lineup_game_coverage ON lineup_game_log(lineup_id, game_id, minutes_played, plus_minus);

-- Player Tracking Coverage
CREATE INDEX IF NOT EXISTS idx_player_tracking_coverage ON player_game_tracking(player_id, game_id, minutes_played, distance_miles);

-- Possession Flow Coverage
CREATE INDEX IF NOT EXISTS idx_possession_flow ON possession(game_id, period, possession_number, start_time, end_time, points_scored);

-- Team Other Stats Coverage
CREATE INDEX IF NOT EXISTS idx_team_other_stats_coverage ON team_game_other_stats(game_id, team_id, points_paint, points_fast_break, points_off_turnovers);
