/* Query to find the team that won the most games */
WITH team_wins AS (
    SELECT 
        home_team_id AS team_id,
        COUNT(*) AS wins
    FROM games
    WHERE home_team_wins = 1
    GROUP BY home_team_id
    UNION ALL
    SELECT 
        visitor_team_id AS team_id,
        COUNT(*) AS wins
    FROM games
    WHERE home_team_wins = 0
    GROUP BY visitor_team_id
),
team_win_totals AS (
    SELECT 
        team_id,
        SUM(wins) AS total_wins
    FROM team_wins
    GROUP BY team_id
)
/* Select the team with the most wins */
SELECT 
    team_id,
    total_wins
FROM team_win_totals
ORDER BY total_wins DESC
LIMIT 1;
