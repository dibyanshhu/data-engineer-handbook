
CREATE TYPE films AS(
		 film TEXT
		,votes INTEGER
		,rating REAL
		,filmid TEXT
	);

CREATE TYPE quality_class AS ENUM('star','good','average','bad');

CREATE TABLE actors (
		actor TEXT
		,current_year INTEGER
		,films films[]
		,quality_class quality_class[]
		,is_active BOOLEAN
		,PRIMARY KEY (actor, current_year)
	);