
/* Query to find the player with the most points for a single team */
WITH player_team_points AS (
    SELECT 
        gd.player_name,
        gd.team_abbreviation,
        SUM(gd.pts) AS total_points
    FROM game_details gd
    JOIN player_seasons ps
    ON gd.player_name = ps.player_name
    WHERE gd.team_abbreviation IS NOT NULL
    AND gd.pts IS NOT NULL
    GROUP BY gd.player_name, gd.team_abbreviation
),
ranked_player_points AS (
    SELECT 
        player_name,
        team_abbreviation,
        total_points,
        RANK() OVER (ORDER BY total_points DESC) AS rank_points
    FROM player_team_points
)
/* Select the top player by total points scored for a single team */
SELECT 
    player_name,
    team_abbreviation,
    total_points
FROM ranked_player_points
WHERE rank_points = 1
;