
/*This query calculates the longest scoring streak for a player, in this case, LeBron James.*/


WITH scoring_streaks AS (
    SELECT 
        player_name,
        game_id,
        pts,
        CASE 
            WHEN pts > 10 THEN 1
            ELSE 0
        END AS scored_over_10,
        SUM(CASE WHEN pts <= 10 THEN 1 ELSE 0 END) OVER (
            PARTITION BY player_name
            ORDER BY game_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS streak_id
    FROM public.game_details
    WHERE player_name = 'LeBron James'
	),
	streak_count AS (
		SELECT 
		    player_name,
		    COUNT(*) AS streak_count
		FROM scoring_streaks
		WHERE scored_over_10 = 1
		GROUP BY player_name, streak_id
	)
	SELECT 
		player_name
		,MAX(streak_count) AS longest_steak
	FROM streak_count
	GROUP BY player_name
	;
