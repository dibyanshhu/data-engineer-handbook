--SELECT table_name 
--  FROM information_schema.tables 
--  WHERE table_schema = 'public'
--  

--SELECT * FROM player_seasons;

--DROP TYPE season_stats;
--CREATE TYPE season_stats AS (
--                          season INTEGER,
--                          gp INTEGER,
--                          pts REAL,
--                          reb REAL,
--                          ast REAL                          
--                         )

--CREATE TABLE players (
--     player_name TEXT
--     ,height TEXT
--     ,college TEXT
--     ,country TEXT
--     ,draft_year TEXT
--     ,DRAFT_ROUND TEXT    
--     ,draft_number TEXT
--     ,season_stats season_stats[]
--     ,current_season INTEGER
--     ,primary key (player_name, current_season)
--     );
     
--
--INSERT INTO players
--WITH yesterday AS (
--         SELECT * FROM players 
--         WHERE current_season = 2000
--        ),
--   today AS (
--       SELECT * FROM player_seasons 
--       WHERE season = 2001
--       )
--       
--    select 
--    COALESCE (t.player_name, y.player_name) as player_name
--    ,COALESCE (t.height, y.height) as height
--    ,COALESCE (t.college, y.college) as college
--    ,COALESCE (t.country, y.country) as country
--    ,COALESCE (t.draft_year, y.draft_year) as draft_year
--    ,COALESCE (t.DRAFT_ROUND, y.DRAFT_ROUND) as DRAFT_ROUND
--    ,COALESCE (t.draft_number, y.draft_number) as draft_number
--    ,CASE WHEN y.season_stats IS NULL
--         THEN ARRAY[ROW(t.season
--                        ,t.gp
--                        ,t.pts
--                        ,t.reb
--                        ,t.ast)::season_stats]
--         WHEN t.season IS NOT NULL THEN y.season_stats || (ARRAY[ROW(t.season
--                        ,t.gp
--                        ,t.pts
--                        ,t.reb
--                        ,t.ast)::season_stats])
--          ELSE y.season_stats
--          END AS  season_stats
--      ,COALESCE(t.season, y.current_season+1) AS current_season
--    
--    from today as t full outer join yesterday as y  
--    on t.player_name = y.player_name;
--   
--   SELECT * FROM players where player_name = 'Michael Jordan' and current_season = 2001; 
--  
--  WITH unnested AS (
--  SELECT player_name, UNNEST(season_stats)::season_stats as season_stats
--  from players where current_season = 2001 --player_name = 'Michael Jordan' and 
--  )
--  
--  select player_name, (season_stats::season_stats).* from unnested;
   
 ------------------------------------------------------------------------------------
 
 --PART 2
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
--     ,primary key (player_name, current_season)
--     );
     

INSERT INTO players
WITH yesterday AS (
         SELECT * FROM players 
         WHERE current_season = 2000
        ),
   today AS (
       SELECT * FROM player_seasons 
       WHERE season = 2001
       )
       
    select 
    COALESCE (t.player_name, y.player_name) as player_name
    ,COALESCE (t.height, y.height) as height
    ,COALESCE (t.college, y.college) as college
    ,COALESCE (t.country, y.country) as country
    ,COALESCE (t.draft_year, y.draft_year) as draft_year
    ,COALESCE (t.DRAFT_ROUND, y.DRAFT_ROUND) as DRAFT_ROUND
    ,COALESCE (t.draft_number, y.draft_number) as draft_number
    ,CASE WHEN y.season_stats IS NULL
         THEN ARRAY[ROW(t.season
                        ,t.gp
                        ,t.pts
                        ,t.reb
                        ,t.ast)::season_stats]
         WHEN t.season IS NOT NULL THEN y.season_stats || (ARRAY[ROW(t.season
                        ,t.gp
                        ,t.pts
                        ,t.reb
                        ,t.ast)::season_stats])
          ELSE y.season_stats
          END AS  season_stats
      ,CASE 
        WHEN t.season IS NOT NULL THEN 
               ARRAY[CASE WHEN t.pts > 20 then 'star'
                 WHEN t.pts > 15 then 'good'
                 WHEN t.pts > 10 then 'average'
                 ELSE 'bad'
               END]::scoring_class[]
            ELSE y.scoring_class 
       END AS scoring_class
      ,CASE WHEN t.season IS NOT NULL THEN 0
            ELSE y.year_since_last_season + 1
            END AS year_since_last_season
      ,COALESCE(t.season, y.current_season+1) AS current_season
    
    FROM today AS t FULL OUTER JOIN yesterday AS y  
    ON t.player_name = y.player_name;
  
   SELECT  player_name,
    (season_stats[1]::season_stats).pts,
    (season_stats[cardinality(season_stats)]::season_stats).pts as latest_season
  FROM players 
 WHERE current_season = 2001 ; 



   SELECT  player_name,
    (season_stats[1]::season_stats).pts/
    case when (season_stats[cardinality(season_stats)]::season_stats).pts = 0 then 1 else (season_stats[cardinality(season_stats)]::season_stats).pts end
  FROM players 
 WHERE current_season = 2001 ; 


