
CREATE TABLE fct_game_details(
	dim_game_date DATE
	,dim_season INTEGER
	,dim_team_id INTEGER
	,dim_player_id INTEGER
	,dim_player_name TEXT
	,dim_start_position TEXT
	,dim_is_playing_at_home BOOLEAN
	,dim_did_not_play BOOLEAN
	,dim_did_not_dress BOOLEAN
	,dim_not_with_team BOOLEAN
	,m_minutes REAL
	,m_fgm INTEGER
	,m_fga INTEGER
	,m_fg3m INTEGER
	,m_fg3a INTEGER
	,m_ftm INTEGER
	,m_fta INTEGER
	,m_oreb INTEGER
	,m_dreb INTEGER
	,m_reb INTEGER
	,m_ast INTEGER
	,m_stl INTEGER
	,m_blk INTEGER
	,m_turnover INTEGER
	,m_pf INTEGER
	,m_pts INTEGER
	,m_plus_minus INTEGER
	,PRIMARY KEY (dim_game_date, dim_team_id,dim_player_id)
);

INSERT INTO fct_game_details 
-- create a filter to get rid of duplicate
WITH deduped AS(
	SELECT g.game_date_est
		,g.season 
		,g.home_team_id 
		,g.visitor_team_id 
		,gd.*
		, ROW_NUMBER() OVER(PARTITION BY gd.game_id, gd.team_id,gd.player_id ORDER BY g.game_date_est) as row_num
	FROM game_details gd
		JOIN games g 
		ON gd.game_id = g.game_id
	)
SELECT 
	game_date_est AS dim_game_date
	,season AS dim_season
	,team_id as dim_team_id
	,player_id as dim_player_id
	,player_name as dim_dim_player_name
	,start_position AS dim_start_position
	,team_id = home_team_id AS dim_is_playing_at_home	
	,COALESCE(POSITION('DNP' in comment),0) > 0 AS dim_did_not_play
	,COALESCE(POSITION('DND' in comment),0) > 0 AS dim_did_not_dress
	,COALESCE(POSITION('NWT' in comment),0) > 0 AS dim_not_with_team
--	,SPLIT_PART(min,':',1) AS minutes
--	,SPLIT_PART(min,':',2) AS seconds
	,CAST(SPLIT_PART(min,':',1) AS REAL)
		+ CAST(SPLIT_PART(min,':',2) AS REAL)/60 AS m_minutes
	,fgm AS m_fgm 
	,fga AS m_fga
	,fg3m as m_fg3m
	,fg3a as m_fg3a
	,ftm as m_ftm
	,fta as m_fta
	,oreb as m_oreb
	,dreb as m_dreb
	,reb as m_reb
	,ast as m_ast
	,stl as m_stl
	,"TO" AS m_turnonvers
	,pf as m_pf
	,pts as m_pts
	,plus_minus as m_plus_minus
	
FROM deduped
WHERE row_num =1;



select dim_player_name
	,count(1) as num_game
	,count(case when dim_not_with_team THEN 1 END) AS bailed_num
	,CAST(count(case when dim_not_with_team THEN 1 END) AS REAL)/CAST(count(1) AS REAL) AS bailed_percent
from fct_game_details  
group by 1
order by 4 desc;