
--CTE used to remove the duplicate records based on the game date
WITH deduped AS(
	SELECT g.game_date_est
		,g.season 
		,g.home_team_id 
		,g.visitor_team_id 
		,gd.*
		,ROW_NUMBER() OVER(PARTITION BY gd.game_id, gd.team_id,gd.player_id ORDER BY g.game_date_est) AS row_num
	FROM game_details gd
		JOIN games g 
		ON gd.game_id = g.game_id
	)
	
SELECT * FROM deduped WHERE row_num = 1;
--SELECT the row no 1 to remove the duplicate records. 