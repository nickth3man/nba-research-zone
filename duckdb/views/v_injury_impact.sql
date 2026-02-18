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
            SELECT AVG(pss.pts)
            FROM sqlite_db.player_season_stats AS pss
            WHERE pss.player_id = i.player_id
                AND pss.season_id <= (
                    SELECT MIN(s.season_id)
                    FROM sqlite_db.season AS s
                    WHERE s.season_label LIKE '%' || CAST(strftime('%Y', CAST(i.injury_date AS DATE)) AS TEXT) || '%'
                )
        ) AS avg_pts_before_injury,
        -- Games affected in current season
        (
            SELECT COUNT(DISTINCT g.game_id)
            FROM sqlite_db.game AS g
            INNER JOIN sqlite_db.player_game_log AS pgl ON g.game_id = pgl.game_id
            WHERE pgl.player_id = i.player_id
                AND pgl.comment IN ('DID NOT PLAY', 'INACTIVE', 'OUT')
                AND g.game_date >= i.injury_date
        ) AS games_affected_current_season
    FROM sqlite_db.injury AS i
    INNER JOIN sqlite_db.player AS p ON i.player_id = p.player_id
    LEFT JOIN sqlite_db.team AS t ON i.team_id = t.team_id
    WHERE i.status = 'Out' OR i.status = 'Day-to-Day' OR i.games_missed > 0
),

team_injury_summary AS (
    SELECT
        inj.team_id,
        inj.team_name,
        COUNT(*) AS total_injuries,
        SUM(inj.games_missed) AS total_games_missed,
        AVG(inj.games_missed) AS avg_games_per_injury,
        SUM(CASE WHEN inj.status = 'Out' THEN 1 ELSE 0 END) AS current_out,
        SUM(CASE WHEN inj.status = 'Day-to-Day' THEN 1 ELSE 0 END) AS current_day_to_day
    FROM injury_status AS inj
    GROUP BY inj.team_id, inj.team_name
)

SELECT
    i.*,
    ti.total_injuries AS team_total_injuries,
    ti.total_games_missed AS team_total_games_missed
FROM injury_status AS i
INNER JOIN team_injury_summary AS ti ON i.team_id = ti.team_id
ORDER BY i.injury_date DESC
