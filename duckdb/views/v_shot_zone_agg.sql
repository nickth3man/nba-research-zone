-- Shot Zone Aggregation View
-- Aggregates shot chart data by zone for all players

SELECT
    sc.player_id,
    p.full_name,
    sc.shot_zone_basic,
    sc.shot_zone_area,
    sc.shot_zone_range,
    COUNT(*) as total_shots,
    SUM(CASE WHEN sc.shot_made_flag = 1 THEN 1 ELSE 0 END) as shots_made,
    SUM(CASE WHEN sc.shot_made_flag = 0 THEN 1 ELSE 0 END) as shots_missed,
    CASE
        WHEN COUNT(*) > 0 THEN SUM(CASE WHEN sc.shot_made_flag = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
    END as fg_pct,
    -- Average shot distance
    AVG(sc.shot_distance) as avg_distance,
    -- Breakdown by shot type
    SUM(CASE WHEN sc.shot_type = '2PT Field Goal' THEN 1 ELSE 0 END) as two_pt_attempts,
    SUM(CASE WHEN sc.shot_type = '2PT Field Goal' AND sc.shot_made_flag = 1 THEN 1 ELSE 0 END) as two_pt_made,
    SUM(CASE WHEN sc.shot_type = '3PT Field Goal' THEN 1 ELSE 0 END) as three_pt_attempts,
    SUM(CASE WHEN sc.shot_type = '3PT Field Goal' AND sc.shot_made_flag = 1 THEN 1 ELSE 0 END) as three_pt_made
FROM sqlite_db.shot_chart AS sc
INNER JOIN sqlite_db.player AS p ON sc.player_id = p.player_id
GROUP BY sc.player_id, p.full_name, sc.shot_zone_basic, sc.shot_zone_area, sc.shot_zone_range
HAVING COUNT(*) >= 50  -- Only zones with significant sample size
ORDER BY sc.player_id ASC, fg_pct DESC
