-- Player Career Statistics View
-- Aggregates player stats across all seasons for career totals

SELECT
    p.player_id,
    p.full_name,
    p.position,
    p.primary_position,
    p.from_year,
    p.to_year,
    SUM(pss.games_played) as career_games,
    SUM(pss.minutes_played) as career_minutes,
    SUM(pss.fgm) as career_fgm,
    SUM(pss.fga) as career_fga,
    CASE
        WHEN SUM(pss.fga) > 0 THEN SUM(pss.fgm) * 1.0 / SUM(pss.fga)
    END as career_fg_pct,
    SUM(pss.fg3m) as career_fg3m,
    SUM(pss.fg3a) as career_fg3a,
    CASE
        WHEN SUM(pss.fg3a) > 0 THEN SUM(pss.fg3m) * 1.0 / SUM(pss.fg3a)
    END as career_fg3_pct,
    SUM(pss.ftm) as career_ftm,
    SUM(pss.fta) as career_fta,
    CASE
        WHEN SUM(pss.fta) > 0 THEN SUM(pss.ftm) * 1.0 / SUM(pss.fta)
    END as career_ft_pct,
    SUM(pss.oreb) as career_oreb,
    SUM(pss.dreb) as career_dreb,
    SUM(pss.reb) as career_reb,
    SUM(pss.ast) as career_ast,
    SUM(pss.stl) as career_stl,
    SUM(pss.blk) as career_blk,
    SUM(pss.tov) as career_tov,
    SUM(pss.pf) as career_pf,
    SUM(pss.pts) as career_pts,
    -- Per-game averages
    CASE
        WHEN SUM(pss.games_played) > 0 THEN SUM(pss.pts) * 1.0 / SUM(pss.games_played)
    END as avg_pts,
    CASE
        WHEN SUM(pss.games_played) > 0 THEN SUM(pss.reb) * 1.0 / SUM(pss.games_played)
    END as avg_reb,
    CASE
        WHEN SUM(pss.games_played) > 0 THEN SUM(pss.ast) * 1.0 / SUM(pss.games_played)
    END as avg_ast,
    -- Advanced stats
    AVG(pss.off_rating) as avg_off_rating,
    AVG(pss.def_rating) as avg_def_rating,
    AVG(pss.net_rating) as avg_net_rating,
    AVG(pss.ts_pct) as avg_ts_pct,
    AVG(pss.efg_pct) as avg_efg_pct,
    AVG(pss.usg_pct) as avg_usg_pct,
    SUM(pss.ws) as career_ws,
    AVG(pss.per) as avg_per
FROM sqlite_db.player AS p
LEFT JOIN sqlite_db.player_season_stats AS pss
    ON p.player_id = pss.player_id
WHERE pss.stat_type = 'Regular Season'
    AND pss.team_id != 0  -- Exclude TOT aggregate rows
GROUP BY
    p.player_id,
    p.full_name,
    p.position,
    p.primary_position,
    p.from_year,
    p.to_year
ORDER BY career_pts DESC
