-- Game Officials View
-- Shows official assignments for each game

SELECT
    g.game_id,
    g.game_date,
    g.season_id,
    s.season_label,
    g.game_type,
    ht.team_name as home_team,
    vt.team_name as away_team,
    o.official_id,
    o.full_name as official_name,
    go.assignment
FROM sqlite_db.game AS g
INNER JOIN sqlite_db.season AS s ON g.season_id = s.season_id
INNER JOIN sqlite_db.team AS ht ON g.home_team_id = ht.team_id
INNER JOIN sqlite_db.team AS vt ON g.away_team_id = vt.team_id
INNER JOIN sqlite_db.game_official AS go ON g.game_id = go.game_id
INNER JOIN sqlite_db.official AS o ON go.official_id = o.official_id
WHERE (g.data_availability_flags & 16) = 16  -- Has officials data
ORDER BY g.game_date DESC, g.game_id ASC
