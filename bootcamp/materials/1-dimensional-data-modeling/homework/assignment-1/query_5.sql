
	WITH last_actor_scd AS (
		SELECT * FROM actors_history_scd 
		where current_year = 2021
		AND end_date = 2021
	),
	historical_scd AS (
		SELECT actor
				,films
				,is_active
				,start_date
				,end_date
		FROM actors_history_scd 
		where current_year = 2021
		AND end_date < 2021
	),
	this_actor_data AS (
		SELECT * FROM actors
		WHERE current_year = 2022
	),
	unchanged_records AS (
		select ts.actor
			,ts.films
			,ts.is_active
			,ls.start_date
			,ls.current_year AS end_date
		from this_actor_data ts
		join last_actor_scd ls 
		on ts.actor = ls.actor
		where ts.films = ls.films
		and ts.is_active = ls.is_active
	),
	changed_records AS (
		select ts.actor
				,UNNEST(ARRAY[
					ROW(
					    ls.films
					    ,ls.is_active
					    ,ls.start_date
					    ,ls.end_date
						)::scd_type,
					ROW(
					    ts.films
					    ,ts.is_active
					    ,ts.current_year
					    ,ts.current_year
					)::scd_type
					]) AS records
		from this_actor_data ts
		join last_actor_scd ls 
		on ts.actor = ls.actor
		where (ts.films <> ls.films
		or ts.is_active <> ls.is_active)
	),
	unnested_changes_records AS(
		select actor
			  ,(records::scd_type).films
			  ,(records::scd_type).is_active
			  ,(records::scd_type).start_year
			  ,(records::scd_type).end_year
		from changed_records
	),
	new_records AS (
		select ts.actor
			  ,ts.films
			  ,ts.is_active
			  ,ls.start_date
			  ,ls.end_date
		
		from this_actor_data ts
		left join last_actor_scd ls 
		on ts.actor = ls.actor
		where ls.actor is null
		
	)
	
	SELECT * FROM historical_scd
	UNION ALL 
	SELECT * FROM unchanged_records
	UNION ALL
	SELECT * FROM unnested_changes_records
	UNION ALL
	select * from new_records
    ;