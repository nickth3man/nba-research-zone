-- Head-to-Head Records View
-- Shows all-time head-to-head records between franchises

SELECT
    f1.franchise_id AS franchise_1_id,
    f1.current_team_name AS franchise_1_name,
    f2.franchise_id AS franchise_2_id,
    f2.current_team_name AS franchise_2_name,
    COUNT(*) AS total_games,
    SUM(CASE WHEN g.winner_team_id IN (SELECT tm1.team_id FROM sqlite_db.team AS tm1 WHERE tm1.franchise_id = f1.franchise_id AND tm1.season_id = g.season_id) THEN 1 ELSE 0 END) AS franchise_1_wins,
    SUM(CASE WHEN g.winner_team_id IN (SELECT tm2.team_id FROM sqlite_db.team AS tm2 WHERE tm2.franchise_id = f2.franchise_id AND tm2.season_id = g.season_id) THEN 1 ELSE 0 END) AS franchise_2_wins,
    SUM(CASE WHEN g.home_team_id IN (SELECT tm3.team_id FROM sqlite_db.team AS tm3 WHERE tm3.franchise_id = f1.franchise_id AND tm3.season_id = g.season_id) AND g.winner_team_id IN (SELECT tm4.team_id FROM sqlite_db.team AS tm4 WHERE tm4.franchise_id = f1.franchise_id AND tm4.season_id = g.season_id) THEN 1 ELSE 0 END) AS franchise_1_home_wins,
    SUM(CASE WHEN g.away_team_id IN (SELECT tm5.team_id FROM sqlite_db.team AS tm5 WHERE tm5.franchise_id = f1.franchise_id AND tm5.season_id = g.season_id) AND g.winner_team_id IN (SELECT tm6.team_id FROM sqlite_db.team AS tm6 WHERE tm6.franchise_id = f1.franchise_id AND tm6.season_id = g.season_id) THEN 1 ELSE 0 END) AS franchise_1_road_wins,
    -- Regular season vs playoffs
    SUM(CASE WHEN g.game_type = 'Regular Season' THEN 1 ELSE 0 END) AS regular_season_games,
    SUM(CASE WHEN g.game_type = 'Playoffs' THEN 1 ELSE 0 END) AS playoff_games
FROM sqlite_db.franchise AS f1
CROSS JOIN sqlite_db.franchise AS f2
INNER JOIN sqlite_db.team AS t1 ON f1.franchise_id = t1.franchise_id
INNER JOIN sqlite_db.team AS t2 ON f2.franchise_id = t2.franchise_id
INNER JOIN sqlite_db.game AS g ON (
    (t1.team_id = g.home_team_id AND t2.team_id = g.away_team_id)
    OR (t2.team_id = g.home_team_id AND t1.team_id = g.away_team_id)
) AND t1.season_id = g.season_id AND t2.season_id = g.season_id
WHERE f1.franchise_id < f2.franchise_id  -- Avoid duplicates
    AND g.winner_team_id IS NOT NULL
GROUP BY f1.franchise_id, f1.current_team_name, f2.franchise_id, f2.current_team_name
HAVING COUNT(*) > 0
ORDER BY total_games DESC
