CREATE TABLE actors_history_scd (
		actor TEXT
		,current_year INTEGER
		,films films[]
		,quality_class quality_class[]
		,is_active BOOLEAN
		,start_date INTEGER
		,end_date INTEGER
		,PRIMARY KEY (actor, start_date)
);
