-- Team Efficiency Analysis View
-- Shows comprehensive team offensive and defensive efficiency metrics

WITH team_efficiency AS (
    SELECT
        t.team_id,
        t.team_name,
        t.season_id,
        s.season_label,
        -- Traditional records
        COUNT(DISTINCT CASE WHEN g.winner_team_id = t.team_id THEN g.game_id END) as wins,
        COUNT(DISTINCT CASE WHEN g.winner_team_id != t.team_id AND g.winner_team_id IS NOT NULL THEN g.game_id END) as losses,
        CAST(COUNT(DISTINCT CASE WHEN g.winner_team_id = t.team_id THEN g.game_id END) AS FLOAT)
            / NULLIF(COUNT(DISTINCT CASE WHEN g.winner_team_id IS NOT NULL THEN g.game_id END), 0) as win_pct,
        -- Advanced stats from new table
        tsa.off_rating,
        tsa.def_rating,
        tsa.net_rating,
        tsa.pace,
        tsa.effective_fg_pct,
        tsa.turnover_pct,
        tsa.offensive_rebound_pct,
        tsa.free_throw_rate,
        tsa.three_point_rate,
        tsa.true_shooting_pct,
        -- Game by game other stats
        AVG(tos.points_paint) as avg_points_paint,
        AVG(tos.points_second_chance) as avg_points_second_chance,
        AVG(tos.points_fast_break) as avg_points_fast_break,
        AVG(tos.points_off_turnovers) as avg_points_off_turnovers,
        AVG(tos.largest_lead) as avg_largest_lead,
        SUM(tos.lead_changes) as total_lead_changes,
        SUM(tos.times_tied) as total_times_tied
    FROM sqlite_db.team AS t
    INNER JOIN sqlite_db.season AS s ON t.season_id = s.season_id
    LEFT JOIN sqlite_db.game AS g ON (t.team_id = g.home_team_id OR t.team_id = g.away_team_id)
        AND t.season_id = g.season_id
        AND g.game_type = 'Regular Season'
    LEFT JOIN sqlite_db.team_season_advanced AS tsa ON t.team_id = tsa.team_id
        AND t.season_id = tsa.season_id
    LEFT JOIN sqlite_db.team_game_other_stats AS tos ON t.team_id = tos.team_id
        AND t.season_id = tos.season_id
    GROUP BY
        t.team_id, t.team_name, t.season_id, s.season_label,
        tsa.off_rating, tsa.def_rating, tsa.net_rating, tsa.pace,
        tsa.effective_fg_pct, tsa.turnover_pct, tsa.offensive_rebound_pct,
        tsa.free_throw_rate, tsa.three_point_rate, tsa.true_shooting_pct
)

SELECT *
FROM team_efficiency
WHERE wins + losses > 0  -- Only teams that have played games
ORDER BY win_pct DESC, net_rating DESC
