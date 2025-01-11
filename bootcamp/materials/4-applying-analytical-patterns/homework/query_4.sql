/* Query to find the player who scored the most points in a single season */
WITH player_season_points AS (
    SELECT 
        gd.player_name,
        ps.season,
        SUM(gd.pts) AS total_points
    FROM game_details gd
    JOIN player_seasons ps
    ON gd.player_name = ps.player_name
    WHERE ps.season IS NOT NULL
    GROUP BY gd.player_name, ps.season
),
ranked_player_season_points AS (
    SELECT 
        player_name,
        season,
        total_points,
        RANK() OVER (PARTITION BY season ORDER BY total_points DESC) AS rank_points
    FROM player_season_points
)
/* Select the top player by total points in each season */
SELECT 
    player_name,
    season,
    total_points
FROM ranked_player_season_points
WHERE rank_points = 1;
