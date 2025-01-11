
/* Create the table for player state analysis */

CREATE TABLE player_status_change (
     player_name TEXT
     ,last_season INTEGER
     ,current_season INTEGER
     ,player_status TEXT
     ,PRIMARY KEY (player_name)
 );


INSERT INTO player_status_change
WITH previous_season AS (
    SELECT 
        player_name,
        last_season AS last_season_played,
        current_season AS current_season_played,
        player_status 
    FROM player_status_change
    WHERE current_season = 1995
),
/* Extract the new state data of the player */
current_season AS (
    SELECT DISTINCT
        player_name,
        season AS current_season_played
    FROM public.player_seasons
    WHERE season = 1996
),
/* Compare the previous vs. current state data of the player */
state_tracking AS (
    SELECT
        COALESCE(c.player_name, p.player_name) AS player_name,
        COALESCE(p.last_season_played, c.current_season_played) AS last_season,
        COALESCE(c.current_season_played, p.last_season_played) AS current_season,
        CASE
            WHEN p.player_name IS NULL THEN 'New' -- Player entering the league
            WHEN c.player_name IS NULL THEN 
                CASE
                    WHEN p.last_season_played = (SELECT MAX(season) FROM public.player_seasons) THEN 'Retired' -- Player leaving the league
                    ELSE 'Stayed Retired' -- Player not playing but already retired
                END
            WHEN c.current_season_played > p.last_season_played THEN 
                CASE 
                    WHEN EXISTS (
                        SELECT 1 
                        FROM public.player_seasons ps
                        WHERE ps.player_name = p.player_name
                          AND ps.season > p.last_season_played
                    ) THEN 'Returned from Retirement' -- Player comes back after missing seasons
                    ELSE 'Continued Playing' -- Default to continued playing
                END
            ELSE 'Continued Playing' -- Player continues playing without interruptions
        END AS player_status
    FROM previous_season AS p
    FULL OUTER JOIN current_season AS c
    ON p.player_name = c.player_name
)
SELECT * 
FROM state_tracking
ORDER BY player_name;
