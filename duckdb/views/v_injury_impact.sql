-- Injury Impact View
-- Shows player injury status and team impact

WITH injury_status AS (
    SELECT
        i.player_id,
        p.full_name,
        i.team_id,
        t.team_name,
        i.injury_date,
        i.injury_type,
        i.body_part,
        i.status,
        i.games_missed,
        i.return_date,
        i.notes,
        -- Get player stats before/after injury
        (
            SELECT AVG(pts)
            FROM sqlite_db.player_season_stats pss
            WHERE pss.player_id = i.player_id
                AND pss.season_id <= (
                    SELECT MIN(season_id)
                    FROM sqlite_db.season s
                    WHERE s.season_label LIKE '%' || CAST(strftime('%Y', i.injury_date) AS TEXT) || '%'
                )
        ) as avg_pts_before_injury,
        -- Games affected in current season
        (
            SELECT COUNT(DISTINCT g.game_id)
            FROM sqlite_db.game g
            JOIN sqlite_db.player_game_log pgl ON g.game_id = pgl.game_id
            WHERE pgl.player_id = i.player_id
                AND pgl.comment IN ('DID NOT PLAY', 'INACTIVE', 'OUT')
                AND g.game_date >= i.injury_date
        ) as games_affected_current_season
    FROM sqlite_db.injury i
    JOIN sqlite_db.player p ON i.player_id = p.player_id
    LEFT JOIN sqlite_db.team t ON i.team_id = t.team_id
    WHERE i.status = 'Out' OR i.status = 'Day-to-Day' OR i.games_missed > 0
),
team_injury_summary AS (
    SELECT
        team_id,
        team_name,
        COUNT(*) as total_injuries,
        SUM(games_missed) as total_games_missed,
        AVG(games_missed) as avg_games_per_injury,
        SUM(CASE WHEN status = 'Out' THEN 1 ELSE 0 END) as current_out,
        SUM(CASE WHEN status = 'Day-to-Day' THEN 1 ELSE 0 END) as current_day_to_day
    FROM injury_status
    GROUP BY team_id, team_name
)
SELECT
    i.*,
    ti.total_injuries as team_total_injuries,
    ti.total_games_missed as team_total_games_missed
FROM injury_status i
JOIN team_injury_summary ti ON i.team_id = ti.team_id
ORDER BY i.injury_date DESC
