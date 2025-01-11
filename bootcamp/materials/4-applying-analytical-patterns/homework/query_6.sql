
/*team with most wins in 90 game strech*/ 
WITH team_wins AS (
    SELECT 
        g.season,
        gd.team_id,
        gd.team_abbreviation,
        SUM(CASE WHEN g.home_team_wins = 1 AND g.team_id_home = gd.team_id THEN 1
                 WHEN g.home_team_wins = 0 AND g.team_id_away = gd.team_id THEN 1
                 ELSE 0 END) OVER (
                     PARTITION BY gd.team_id
                     ORDER BY g.game_date_est
                     ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
                 ) AS wins_in_90_games
    FROM game_details gd
    JOIN games g
        ON gd.game_id = g.game_id
)
SELECT 
    team_id,
    team_abbreviation,
    MAX(wins_in_90_games) AS max_wins_in_90_game_stretch
FROM team_wins
GROUP BY team_id, team_abbreviation
ORDER BY max_wins_in_90_game_stretch DESC
LIMIT 1;
