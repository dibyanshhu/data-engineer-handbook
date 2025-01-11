
-- Query for game Aggregations using the grouping set
CREATE TABLE game_aggregation AS 
WITH game_group AS (
    SELECT 
        player_name,
        team_abbreviation,
        season,
        SUM(pts) AS total_points,
        SUM(ast) AS total_assists,
        SUM(reb) AS total_rebounds,
        COUNT(DISTINCT gd.game_id) AS games_played,
        GROUPING(player_name, team_abbreviation, season) AS grouping_level
    FROM game_details gd
    JOIN games g
    ON gd.game_id = g.game_id
    GROUP BY GROUPING SETS (
        (player_name, team_abbreviation, season), -- Player, team, and season
        (player_name, team_abbreviation),         -- Player and team
        (player_name, season),                    -- Player and season
        (team_abbreviation),                      -- Team only
        (player_name),                            -- Player only
        (season)                                  -- Season only
    )
    ORDER BY grouping_level, total_points DESC
)
/* Select all from the grouped results */
SELECT * FROM game_group;
