
--select * from generate_series(1996,2022);
 --LAB 2
--DROP TABLE players; 
--CREATE TYPE scoring_class AS ENUM('star','good','average','bad');

--DROP TABLE players ;
--CREATE TABLE players (
--     player_name TEXT
--     ,height TEXT
--     ,college TEXT
--     ,country TEXT
--     ,draft_year TEXT
--     ,DRAFT_ROUND TEXT    
--     ,draft_number TEXT
--     ,season_stats season_stats[]
--     ,scoring_class scoring_class[]
--     ,year_since_last_season INTEGER
--     ,current_season INTEGER
--     ,is_active BOOLEAN
--     ,primary key (player_name, current_season)
--     );
    

-------------------------------------------------------------
--    
--INSERT INTO players
--WITH years AS (
--     SELECT * 
--     FROM generate_series(1996,2022) as season 
--         
--     ),
--     p AS (
--     		SELECT player_name, MIN(season) as first_season 
--       		FROM player_seasons
--       		GROUP BY player_name
--       ),
--     players_and_seasons AS (
--       SELECT *
--       FROM p
--       JOIN years y ON p.first_season <=y.season
--     ),
--     windowed AS (
--    SELECT
--        pas.player_name,
--        pas.season,
--        ARRAY_REMOVE(
--            ARRAY_AGG(
--                CASE
--                    WHEN ps.season IS NOT NULL
--                        THEN ROW(
--                            ps.season,
--                            ps.gp,
--                            ps.pts,
--                            ps.reb,
--                            ps.ast
--                        )::season_stats
--                END)
--            OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
--            NULL
--        ) AS seasons
--    FROM players_and_seasons pas
--    LEFT JOIN player_seasons ps
--        ON pas.player_name = ps.player_name
--        AND pas.season = ps.season
--    ORDER BY pas.player_name, pas.season
--), static AS (
--    SELECT
--        player_name,
--        MAX(height) AS height,
--        MAX(college) AS college,
--        MAX(country) AS country,
--        MAX(draft_year) AS draft_year,
--        MAX(draft_round) AS draft_round,
--        MAX(draft_number) AS draft_number
--    FROM player_seasons
--    GROUP BY player_name
--)
--     
--SELECT
--    w.player_name,
--    s.height,
--    s.college,
--    s.country,
--    s.draft_year,
--    s.draft_round,
--    s.draft_number,
--    seasons AS season_stats,
--    ARRAY[CASE
--        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
--        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
--        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
--        ELSE 'bad'
--    END]::scoring_class[] AS scoring_class,
--    w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_active,
--    w.season,
--    (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active
--FROM windowed w
--JOIN static s
--    ON w.player_name = s.player_name;
    
   
 
------------------------------------------------------------------------------------------------

-------------- SCD TYPE 2 implemented --------------
   
--   SELECT player_name, scoring_class, is_active from players p where current_season=2022;
  
--drop TABLE players_scd; 
--   CREATE TABLE players_scd (
--   			player_name TEXT
--   			,scoring_class scoring_class[]
--   			,is_active BOOLEAN
--   			,start_season INTEGER
--   			,end_season INTEGER
--   			,current_season INTEGER
--   			,PRIMARY KEY (player_name, start_season)   
--   );
  

------------- Load data till in SCD type using the group by approach and also the year can be controlled by current_season-------------------
--
--  INSERT INTO players_scd
--  WITH with_previous AS(
--	  SELECT 
--	       player_name
--	       ,scoring_class
--	       ,current_season
--	       ,is_active
--	       ,LAG(scoring_class,1) over (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class
--	       ,LAG(is_active,1) over (PARTITION BY player_name ORDER BY current_season) as previous_is_active
--	  FROM players p
--	  WHERE current_season <=2021 
--	  ),
--	  with_indicators AS (
--	      	  SELECT * 
----			  		,CASE
----			  		    WHEN scoring_class <> previous_scoring_class then 1
----			  		    ELSE 0
----			  		    END AS scoring_class_change_indicator
----			  		,CASE
----			  		    WHEN is_active <> previous_is_active then 1
----			  		    ELSE 0
----			  		    END AS is_active_change_indicator\
--	      	        ,CASE
--	      	        	WHEN is_active <> previous_is_active THEN 1
--	      	        	WHEN scoring_class <> previous_scoring_class THEN 1
--	      	        	else 0
--	      	        END AS change_indicator
--			   FROM with_previous
--	  ),
--	  with_streaks AS (
--		  		SELECT  *
--		         ,SUM(change_indicator) OVER (PARTITION BY player_name order by current_season) as streak_identifier
--		  		FROM with_indicators
--	  )
--	  
--	  SELECT player_name
--	  		,ARRAY[scoring_class]::scoring_class[] AS scoring_class
--	  		,is_active
--	  		,MIN(current_season) as start_season
--	  		,MAX(current_season) as end_season
--	  		,2021 AS current_season
--	  FROM with_streaks
--	  GROUP BY 1,2,3
--	  order by 1,2
--	  ;
	   



-------------------- Approach 2 adding the incremental records  -------------------------

--CREATE TYPE scd_type AS (
--		scoring_class scoring_class[]
--		,is_active BOOLEAN
--		,start_season INTEGER
--		,end_season INTEGER
--);


	WITH last_season_scd AS (
		SELECT * FROM players_scd 
		where current_season = 2021
		AND end_season = 2021
	),
		historical_scd AS (
			SELECT player_name
				   ,scoring_class
				   ,is_active
				   ,start_season
				   ,end_season
			FROM players_scd 
			where current_season = 2021
			AND end_season < 2021
	),
	
	this_season_data AS (
		SELECT * FROM players
		WHERE current_season =2022
	),
	unchanged_records AS (
		select ts.player_name
			,ts.scoring_class
			,ts.is_active
			,ls.start_season
			,ls.current_season AS end_season
		from this_season_data ts
		join last_season_scd ls 
		on ts.player_name = ls.player_name
--		where ts.scoring_class = ls.scoring_class
--		and ts.is_active = ls.is_active
	),
	changed_records AS (
		select ts.player_name
				,UNNEST(ARRAY[
					ROW(
					    ls.scoring_class
					    ,ls.is_active
					    ,ls.start_season
					    ,ls.end_season
						)::scd_type,
					ROW(
					    ts.scoring_class
					    ,ts.is_active
					    ,ts.current_season
					    ,ts.current_season
					)::scd_type
					]) AS records
			from this_season_data ts
			join last_season_scd ls 
			on ts.player_name = ls.player_name
			where (ts.scoring_class <> ls.scoring_class
			or ts.is_active <> ls.is_active)
	),
	unnested_changes_records AS(
		select player_name
			  ,(records::scd_type).scoring_class
			  ,(records::scd_type).is_active
			  ,(records::scd_type).start_season
			  ,(records::scd_type).end_season
		from changed_records
	),
	new_records AS (
		select ts.player_name
			  ,ts.scoring_class
			  ,ts.is_active
			  ,ls.start_season
			  ,ls.end_season
		
		from this_season_data ts
		left join last_season_scd ls 
		on ts.player_name = ls.player_name
		where ls.player_name is null
		
	)
	
	SELECT * FROM historical_scd
	UNION ALL 
	SELECT * FROM unchanged_records
	UNION ALL
	SELECT * FROM unnested_changes_records
	UNION ALL
	select * from new_records
    ;
