-- Player Advanced Complete View
-- Shows comprehensive player statistics including new advanced metrics

WITH player_advanced AS (
    SELECT
        p.player_id,
        p.full_name,
        p.position,
        p.primary_position,
        p.from_year,
        p.to_year,
        pss.team_id,
        t.team_name,
        pss.season_id,
        s.season_label,
        pss.stat_type,
        -- Traditional stats
        pss.games_played,
        pss.games_started,
        pss.minutes_played,
        pss.pts,
        pss.reb,
        pss.ast,
        pss.stl,
        pss.blk,
        pss.tov,
        -- Per game averages
        CASE WHEN pss.games_played > 0 THEN pss.pts / pss.games_played ELSE NULL END as ppg,
        CASE WHEN pss.games_played > 0 THEN pss.reb / pss.games_played ELSE NULL END as rpg,
        CASE WHEN pss.games_played > 0 THEN pss.ast / pss.games_played ELSE NULL END as apg,
        -- Advanced stats (existing)
        pss.off_rating,
        pss.def_rating,
        pss.net_rating,
        pss.ts_pct,
        pss.efg_pct,
        pss.usg_pct,
        pss.per,
        pss.ws,
        pss.bpm,
        pss.vorp,
        -- New advanced metrics
        pss.ows,
        pss.dws,
        pss.obpm,
        pss.dbpm,
        pss.three_point_attempt_rate,
        pss.free_throw_rate,
        pss.ws_per_48,
        -- Efficiency metrics
        CASE WHEN pss.minutes_played > 0 THEN (pss.pts + pss.reb + pss.ast + pss.stl + pss.blk -
            (pss.fga * 0) - (pss.fta * 0) - pss.tov) / pss.games_played ELSE NULL END as efficiency_per_game,
        -- Scoring efficiency
        CASE WHEN pss.fga > 0 THEN pss.pts / pss.fga ELSE NULL END as points_per_fga,
        CASE WHEN pss.fga + 0.44 * pss.fta > 0 THEN pss.pts / (pss.fga + 0.44 * pss.fta) ELSE NULL END as true_shooting_calc
    FROM sqlite_db.player p
    JOIN sqlite_db.player_season_stats pss ON p.player_id = pss.player_id
    JOIN sqlite_db.team t ON pss.team_id = t.team_id
    JOIN sqlite_db.season s ON pss.season_id = s.season_id
    WHERE pss.stat_type = 'Regular Season'
        AND pss.team_id != 0  -- Exclude TOT aggregate rows
)
SELECT
    *
FROM player_advanced
WHERE games_played >= 10  -- Only players with significant playing time
ORDER BY per DESC
