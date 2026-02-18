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
        CASE WHEN pss.games_played > 0 THEN pss.pts * 1.0 / pss.games_played ELSE NULL END as ppg,
        CASE WHEN pss.games_played > 0 THEN pss.reb * 1.0 / pss.games_played ELSE NULL END as rpg,
        CASE WHEN pss.games_played > 0 THEN pss.ast * 1.0 / pss.games_played ELSE NULL END as apg,
        CASE WHEN pss.games_played > 0 THEN pss.stl * 1.0 / pss.games_played ELSE NULL END as spg,
        CASE WHEN pss.games_played > 0 THEN pss.blk * 1.0 / pss.games_played ELSE NULL END as bpg
    FROM sqlite_db.player_season_stats pss
    JOIN sqlite_db.player p ON pss.player_id = p.player_id
    JOIN sqlite_db.team t ON pss.team_id = t.team_id
    JOIN sqlite_db.season s ON pss.season_id = s.season_id
    WHERE pss.stat_type = 'Regular Season'
        AND pss.team_id != 0  -- Exclude TOT aggregate rows
        AND pss.games_played >= 58  -- Qualify for leaderboards (approx 70% of season)
),
ranked_stats AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY season_id ORDER BY ppg DESC) as pts_rank,
        RANK() OVER (PARTITION BY season_id ORDER BY rpg DESC) as reb_rank,
        RANK() OVER (PARTITION BY season_id ORDER BY apg DESC) as ast_rank,
        RANK() OVER (PARTITION BY season_id ORDER BY spg DESC) as stl_rank,
        RANK() OVER (PARTITION BY season_id ORDER BY bpg DESC) as blk_rank
    FROM season_totals
)
SELECT
    season_id,
    season_label,
    -- Points leader
    (SELECT full_name FROM ranked_stats WHERE pts_rank = 1 AND season_id = r.season_id LIMIT 1) as pts_leader,
    (SELECT ppg FROM ranked_stats WHERE pts_rank = 1 AND season_id = r.season_id LIMIT 1) as pts_leader_avg,
    -- Rebounds leader
    (SELECT full_name FROM ranked_stats WHERE reb_rank = 1 AND season_id = r.season_id LIMIT 1) as reb_leader,
    (SELECT rpg FROM ranked_stats WHERE reb_rank = 1 AND season_id = r.season_id LIMIT 1) as reb_leader_avg,
    -- Assists leader
    (SELECT full_name FROM ranked_stats WHERE ast_rank = 1 AND season_id = r.season_id LIMIT 1) as ast_leader,
    (SELECT apg FROM ranked_stats WHERE ast_rank = 1 AND season_id = r.season_id LIMIT 1) as ast_leader_avg,
    -- Steals leader
    (SELECT full_name FROM ranked_stats WHERE stl_rank = 1 AND season_id = r.season_id LIMIT 1) as stl_leader,
    (SELECT spg FROM ranked_stats WHERE stl_rank = 1 AND season_id = r.season_id LIMIT 1) as stl_leader_avg,
    -- Blocks leader
    (SELECT full_name FROM ranked_stats WHERE blk_rank = 1 AND season_id = r.season_id LIMIT 1) as blk_leader,
    (SELECT bpg FROM ranked_stats WHERE blk_rank = 1 AND season_id = r.season_id LIMIT 1) as blk_leader_avg
FROM (SELECT DISTINCT season_id, season_label FROM season_totals) r
ORDER BY season_id DESC
