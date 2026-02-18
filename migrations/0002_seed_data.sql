-- Seed data for NBA Vault database
-- This migration inserts initial reference data

-- Insert league data
INSERT INTO league (league_id, league_name, founded_year, folded_year) VALUES
    ('BAA', 'Basketball Association of America', 1946, 1949),
    ('NBA', 'National Basketball Association', 1949, NULL),
    ('ABA', 'American Basketball Association', 1967, 1976)
ON CONFLICT (league_id) DO NOTHING;

-- Insert data availability flag definitions
INSERT INTO data_availability_flag_def (bit_position, bit_value, flag_name, description) VALUES
    (0,   1,   'BOXSCORE_TRADITIONAL', 'Traditional team + player box score loaded'),
    (1,   2,   'BOXSCORE_ADVANCED',    'Advanced box score loaded'),
    (2,   4,   'PLAY_BY_PLAY',         'Play-by-play events loaded (~1996-97 onward)'),
    (3,   8,   'SHOT_CHART',           'Shot chart coordinates loaded (~1996-97 onward)'),
    (4,   16,  'OFFICIALS',            'Referee assignments loaded (~1990-91 onward)'),
    (5,   32,  'HUSTLE_STATS',         'Hustle stats loaded (2015-16 onward)'),
    (6,   64,  'TRACKING_STATS',       'Speed/distance tracking loaded (2013-14 onward)')
ON CONFLICT (flag_name) DO NOTHING;

-- Insert event message type definitions (NBA.com event_msg_type values)
INSERT INTO event_message_type (event_type_id, event_name) VALUES
    (1,  'Field Goal Made'),
    (2,  'Field Goal Missed'),
    (3,  'Free Throw Made'),
    (4,  'Free Throw Missed'),
    (5,  'Rebound Offensive'),
    (6,  'Rebound Defensive'),
    (7,  'Turnover'),
    (8,  'Foul Personal'),
    (9,  'Foul Shooting'),
    (10, 'Foul Offensive'),
    (11, 'Foul Technical'),
    (12, 'Foul Flagrant 1'),
    (13, 'Foul Flagrant 2'),
    (14, 'Violation'),
    (15, 'Timeout'),
    (16, 'Jump Ball'),
    (17, 'Ejection'),
    (18, 'Period Begin'),
    (19, 'Period End'),
    (20, 'Substitution'),
    (21, 'Game End')
ON CONFLICT (event_type_id) DO NOTHING;

-- Insert sentinel TOT team record for player_season_stats aggregate rows
-- This will be referenced when inserting player stats after team data is loaded
-- Note: team_id=0 is a special sentinel value
-- The actual row will be inserted after the team table has proper seasons
