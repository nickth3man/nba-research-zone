-- Franchise Timeline View
-- Shows franchise history including relocations and name changes

SELECT
    f.franchise_id,
    f.current_team_name,
    f.current_city,
    f.abbreviation,
    f.league_id,
    f.founded_year,
    t.season_id,
    s.season_label,
    t.team_name,
    t.city,
    t.conference,
    t.division,
    t.arena_name,
    -- Get team record for the season
    (
        SELECT COUNT(*)
        FROM sqlite_db.game g
        WHERE (g.home_team_id = t.team_id OR g.away_team_id = t.team_id)
            AND g.season_id = t.season_id
            AND g.game_type = 'Regular Season'
            AND g.winner_team_id IS NOT NULL
    ) as games_played,
    (
        SELECT COUNT(*)
        FROM sqlite_db.game g
        WHERE g.winner_team_id = t.team_id
            AND g.season_id = t.season_id
            AND g.game_type = 'Regular Season'
    ) as wins,
    -- Playoff appearances
    (
        SELECT COUNT(*) > 0
        FROM sqlite_db.game g
        WHERE (g.home_team_id = t.team_id OR g.away_team_id = t.team_id)
            AND g.season_id = t.season_id
            AND g.game_type = 'Playoffs'
    ) as made_playoffs
FROM sqlite_db.franchise f
JOIN sqlite_db.team t ON f.franchise_id = t.franchise_id
JOIN sqlite_db.season s ON t.season_id = s.season_id
ORDER BY f.franchise_id, s.season_id
