-- Player Tracking Summary View
-- Shows comprehensive player movement and efficiency metrics

WITH tracking_summary AS (
    SELECT
        pgt.player_id,
        p.full_name,
        pgt.team_id,
        t.team_name,
        pgt.season_id,
        s.season_label,
        -- Games played
        COUNT(DISTINCT pgt.game_id) as games_played,
        -- Total tracking stats
        SUM(pgt.minutes_played) as total_minutes,
        SUM(pgt.distance_miles) as total_distance_miles,
        AVG(pgt.distance_miles) as avg_distance_miles_per_game,
        AVG(pgt.speed_mph_avg) as avg_speed_mph,
        MAX(pgt.speed_mph_max) as max_speed_mph,
        SUM(pgt.touches) as total_touches,
        AVG(pgt.touches) as avg_touches_per_game,
        SUM(pgt.touches_catch_shoot) as total_catch_shoot_touches,
        SUM(pgt.touches_paint) as total_paint_touches,
        SUM(pgt.touches_post_up) as total_post_up_touches,
        SUM(pgt.drives) as total_drives,
        SUM(pgt.drives_pts) as total_drive_points,
        AVG(pgt.drives_pts) as avg_drive_points_per_game,
        SUM(pgt.pull_up_shots) as total_pull_up_shots,
        SUM(pgt.pull_up_shots_made) as total_pull_up_shots_made,
        CASE
            WHEN SUM(pgt.pull_up_shots) > 0 THEN
                SUM(pgt.pull_up_shots_made) * 100.0 / SUM(pgt.pull_up_shots)
        END as pull_up_shooting_pct,
        -- Per 36 minutes metrics
        (SUM(pgt.distance_miles) * 36.0 / NULLIF(SUM(pgt.minutes_played), 0)) as distance_miles_per_36,
        (SUM(pgt.touches) * 36.0 / NULLIF(SUM(pgt.minutes_played), 0)) as touches_per_36,
        (SUM(pgt.drives) * 36.0 / NULLIF(SUM(pgt.minutes_played), 0)) as drives_per_36
    FROM sqlite_db.player_game_tracking AS pgt
    INNER JOIN sqlite_db.player AS p ON pgt.player_id = p.player_id
    INNER JOIN sqlite_db.team AS t ON pgt.team_id = t.team_id
    INNER JOIN sqlite_db.season AS s ON pgt.season_id = s.season_id
    GROUP BY
        pgt.player_id, p.full_name, pgt.team_id, t.team_name,
        pgt.season_id, s.season_label
)

SELECT *
FROM tracking_summary
WHERE total_minutes >= 100  -- Only players with significant tracking time
ORDER BY total_distance_miles DESC
