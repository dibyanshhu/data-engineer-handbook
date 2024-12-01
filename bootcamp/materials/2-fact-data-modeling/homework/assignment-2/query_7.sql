-- A monthly, reduced fact table DDL host_activity_reduced
CREATE TABLE host_activity_reduced(		
		month_start DATE
		,host TEXT
		,hit_array INTEGER -- think COUNT(1)
		,unique_visitors INTEGER[]  --array - think COUNT(DISTINCT user_id)
		,PRIMARY KEY(month_start, host)
		);