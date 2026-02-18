-- Head-to-Head Records View
-- Shows all-time head-to-head records between franchises

SELECT
    f1.franchise_id as franchise_1_id,
    f1.current_team_name as franchise_1_name,
    f2.franchise_id as franchise_2_id,
    f2.current_team_name as franchise_2_name,
    COUNT(*) as total_games,
    SUM(CASE WHEN g.winner_team_id IN (SELECT team_id FROM sqlite_db.team WHERE franchise_id = f1.franchise_id AND season_id = g.season_id) THEN 1 ELSE 0 END) as franchise_1_wins,
    SUM(CASE WHEN g.winner_team_id IN (SELECT team_id FROM sqlite_db.team WHERE franchise_id = f2.franchise_id AND season_id = g.season_id) THEN 1 ELSE 0 END) as franchise_2_wins,
    SUM(CASE WHEN g.home_team_id IN (SELECT team_id FROM sqlite_db.team WHERE franchise_id = f1.franchise_id AND season_id = g.season_id) AND g.winner_team_id IN (SELECT team_id FROM sqlite_db.team WHERE franchise_id = f1.franchise_id AND season_id = g.season_id) THEN 1 ELSE 0 END) as franchise_1_home_wins,
    SUM(CASE WHEN g.away_team_id IN (SELECT team_id FROM sqlite_db.team WHERE franchise_id = f1.franchise_id AND season_id = g.season_id) AND g.winner_team_id IN (SELECT team_id FROM sqlite_db.team WHERE franchise_id = f1.franchise_id AND season_id = g.season_id) THEN 1 ELSE 0 END) as franchise_1_road_wins,
    -- Regular season vs playoffs
    SUM(CASE WHEN g.game_type = 'Regular Season' THEN 1 ELSE 0 END) as regular_season_games,
    SUM(CASE WHEN g.game_type = 'Playoffs' THEN 1 ELSE 0 END) as playoff_games
FROM sqlite_db.franchise f1
CROSS JOIN sqlite_db.franchise f2
JOIN sqlite_db.team t1 ON f1.franchise_id = t1.franchise_id
JOIN sqlite_db.team t2 ON f2.franchise_id = t2.franchise_id
JOIN sqlite_db.game g ON (
    (g.home_team_id = t1.team_id AND g.away_team_id = t2.team_id) OR
    (g.home_team_id = t2.team_id AND g.away_team_id = t1.team_id)
) AND g.season_id = t1.season_id AND g.season_id = t2.season_id
WHERE f1.franchise_id < f2.franchise_id  -- Avoid duplicates
    AND g.winner_team_id IS NOT NULL
GROUP BY f1.franchise_id, f1.current_team_name, f2.franchise_id, f2.current_team_name
HAVING COUNT(*) > 0
ORDER BY total_games DESC
