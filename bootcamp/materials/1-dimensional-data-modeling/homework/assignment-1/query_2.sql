
INSERT INTO actors
	with     yesterday AS (
			SELECT * FROM actors WHERE current_year=1969
		),
		today AS(
			SELECT 
				actor
				,actorid
				,max(year) as year
				,ARRAY_AGG(ARRAY[ROW(
	                film,
	                votes,
	                rating,
	                filmid
	        		)::films]) as films
	        	,avg(rating) as avg_rating
			 FROM actor_films 
			 WHERE YEAR=1970
			 group by actor,actorid
		)
	SELECT 
    COALESCE (t.actor, y.actor) as actor
    ,COALESCE (t.year, y.current_year+1) as current_year
    ,CASE WHEN y.films IS NULL then t.films
          WHEN t.year IS NOT NULL THEN  y.films || t.films
          ELSE y.films
       END AS  films
      ,CASE 
        WHEN t.year IS NOT NULL THEN 
               ARRAY[CASE WHEN avg_rating > 8 then 'star'
	                  	  WHEN avg_rating > 7 then 'good'
    	             	  WHEN avg_rating > 6 then 'average'
        	         ELSE 'bad'
               END]::quality_class[]
            ELSE y.quality_class 
       END AS quality_class
      ,CASE 
      		WHEN t.year IS NOT NULL then TRUE 
      		ELSE FALSE 
      		END AS is_active
    
    FROM today AS t FULL OUTER JOIN yesterday AS y  
    ON t.actor = y.actor;
   