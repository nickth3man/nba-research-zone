-- Season Leaders View
-- Shows statistical leaders for each season

WITH season_totals AS (
    SELECT
        pss.season_id,
        s.season_label,
        pss.player_id,
        p.full_name,
        pss.team_id,
        t.team_name,
        pss.stat_type,
        pss.games_played,
        pss.pts,
        pss.reb,
        pss.ast,
        pss.stl,
        pss.blk,
        pss.minutes_played,
        -- Per-game averages
        CASE WHEN pss.games_played > 0 THEN pss.pts * 1.0 / pss.games_played END AS ppg,
        CASE WHEN pss.games_played > 0 THEN pss.reb * 1.0 / pss.games_played END AS rpg,
        CASE WHEN pss.games_played > 0 THEN pss.ast * 1.0 / pss.games_played END AS apg,
        CASE WHEN pss.games_played > 0 THEN pss.stl * 1.0 / pss.games_played END AS spg,
        CASE WHEN pss.games_played > 0 THEN pss.blk * 1.0 / pss.games_played END AS bpg
    FROM sqlite_db.player_season_stats AS pss
    INNER JOIN sqlite_db.player AS p ON pss.player_id = p.player_id
    INNER JOIN sqlite_db.team AS t ON pss.team_id = t.team_id
    INNER JOIN sqlite_db.season AS s ON pss.season_id = s.season_id
    WHERE pss.stat_type = 'Regular Season'
        AND pss.team_id != 0  -- Exclude TOT aggregate rows
        AND pss.games_played >= 58  -- Qualify for leaderboards (approx 70% of season)
),

ranked_stats AS (
    SELECT
        st.*,
        RANK() OVER (PARTITION BY st.season_id ORDER BY st.ppg DESC) AS pts_rank,
        RANK() OVER (PARTITION BY st.season_id ORDER BY st.rpg DESC) AS reb_rank,
        RANK() OVER (PARTITION BY st.season_id ORDER BY st.apg DESC) AS ast_rank,
        RANK() OVER (PARTITION BY st.season_id ORDER BY st.spg DESC) AS stl_rank,
        RANK() OVER (PARTITION BY st.season_id ORDER BY st.bpg DESC) AS blk_rank
    FROM season_totals AS st
)

SELECT
    r.season_id,
    r.season_label,
    -- Points leader
    (SELECT rs1.full_name FROM ranked_stats AS rs1 WHERE rs1.pts_rank = 1 AND rs1.season_id = r.season_id ORDER BY rs1.player_id LIMIT 1) AS pts_leader,
    (SELECT rs2.ppg FROM ranked_stats AS rs2 WHERE rs2.pts_rank = 1 AND rs2.season_id = r.season_id ORDER BY rs2.player_id LIMIT 1) AS pts_leader_avg,
    -- Rebounds leader
    (SELECT rs3.full_name FROM ranked_stats AS rs3 WHERE rs3.reb_rank = 1 AND rs3.season_id = r.season_id ORDER BY rs3.player_id LIMIT 1) AS reb_leader,
    (SELECT rs4.rpg FROM ranked_stats AS rs4 WHERE rs4.reb_rank = 1 AND rs4.season_id = r.season_id ORDER BY rs4.player_id LIMIT 1) AS reb_leader_avg,
    -- Assists leader
    (SELECT rs5.full_name FROM ranked_stats AS rs5 WHERE rs5.ast_rank = 1 AND rs5.season_id = r.season_id ORDER BY rs5.player_id LIMIT 1) AS ast_leader,
    (SELECT rs6.apg FROM ranked_stats AS rs6 WHERE rs6.ast_rank = 1 AND rs6.season_id = r.season_id ORDER BY rs6.player_id LIMIT 1) AS ast_leader_avg,
    -- Steals leader
    (SELECT rs7.full_name FROM ranked_stats AS rs7 WHERE rs7.stl_rank = 1 AND rs7.season_id = r.season_id ORDER BY rs7.player_id LIMIT 1) AS stl_leader,
    (SELECT rs8.spg FROM ranked_stats AS rs8 WHERE rs8.stl_rank = 1 AND rs8.season_id = r.season_id ORDER BY rs8.player_id LIMIT 1) AS stl_leader_avg,
    -- Blocks leader
    (SELECT rs9.full_name FROM ranked_stats AS rs9 WHERE rs9.blk_rank = 1 AND rs9.season_id = r.season_id ORDER BY rs9.player_id LIMIT 1) AS blk_leader,
    (SELECT rs10.bpg FROM ranked_stats AS rs10 WHERE rs10.blk_rank = 1 AND rs10.season_id = r.season_id ORDER BY rs10.player_id LIMIT 1) AS blk_leader_avg
FROM (SELECT DISTINCT
    r2.season_id,
    r2.season_label
FROM ranked_stats AS r2) AS r
ORDER BY r.season_id DESC
