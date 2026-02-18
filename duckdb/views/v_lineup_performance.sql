-- Lineup Performance View
-- Shows comprehensive lineup statistics and performance metrics

WITH lineup_stats AS (
    SELECT
        l.lineup_id,
        l.season_id,
        s.season_label,
        l.team_id,
        t.team_name,
        l.player_1_id,
        p1.full_name as player_1_name,
        l.player_2_id,
        p2.full_name as player_2_name,
        l.player_3_id,
        p3.full_name as player_3_name,
        l.player_4_id,
        p4.full_name as player_4_name,
        l.player_5_id,
        p5.full_name as player_5_name,
        l.minutes_played as total_minutes,
        l.possessions as total_possessions,
        l.points_scored as total_points_scored,
        l.points_allowed as total_points_allowed,
        l.off_rating,
        l.def_rating,
        l.net_rating,
        -- Calculate games played
        COUNT(DISTINCT lgl.game_id) as games_played,
        -- Average per game stats
        AVG(lgl.minutes_played) as avg_minutes_per_game,
        AVG(lgl.plus_minus) as avg_plus_minus,
        -- Net rating per 100 possessions
        CASE
            WHEN l.possessions > 0 THEN
                ((l.points_scored - l.points_allowed) * 100.0 / l.possessions)
            ELSE NULL
        END as calculated_net_rating,
        -- Win percentage (positive plus-minus as proxy)
        CAST(SUM(CASE WHEN lgl.plus_minus > 0 THEN 1 ELSE 0 END) AS FLOAT) /
            NULLIF(COUNT(DISTINCT lgl.game_id), 0) as win_pct
    FROM sqlite_db.lineup l
    JOIN sqlite_db.team t ON l.team_id = t.team_id
    JOIN sqlite_db.season s ON l.season_id = s.season_id
    LEFT JOIN sqlite_db.lineup_game_log lgl ON l.lineup_id = lgl.lineup_id
    LEFT JOIN sqlite_db.player p1 ON l.player_1_id = p1.player_id
    LEFT JOIN sqlite_db.player p2 ON l.player_2_id = p2.player_id
    LEFT JOIN sqlite_db.player p3 ON l.player_3_id = p3.player_id
    LEFT JOIN sqlite_db.player p4 ON l.player_4_id = p4.player_id
    LEFT JOIN sqlite_db.player p5 ON l.player_5_id = p5.player_id
    GROUP BY
        l.lineup_id, l.season_id, s.season_label, l.team_id, t.team_name,
        l.player_1_id, l.player_2_id, l.player_3_id, l.player_4_id, l.player_5_id,
        p1.full_name, p2.full_name, p3.full_name, p4.full_name, p5.full_name,
        l.minutes_played, l.possessions, l.points_scored, l.points_allowed,
        l.off_rating, l.def_rating, l.net_rating
)
SELECT
    *
FROM lineup_stats
WHERE total_minutes >= 10  -- Only show lineups with significant minutes
ORDER BY calculated_net_rating DESC
