

-- cumulative query to generate device_activity_datelist from events
INSERT INTO user_devices_cumulated
WITH  yesterday AS (
		SELECT * 
		FROM user_devices_cumulated 
		WHERE date = DATE('2023-01-30')
		),
		today AS (
			SELECT 
				e.user_id::TEXT AS user_id
				,d.browser_type
				,e.event_time::DATE AS date_active
			FROM events e
			JOIN devices d 
			ON e.device_id = d.device_id 
			WHERE event_time::DATE = DATE('2023-01-31')
			AND user_id IS NOT NULL AND e.device_id IS NOT NULL
			GROUP BY user_id ,d.browser_type , event_time::DATE 
			ORDER BY user_id
		)
		SELECT 
			COALESCE(t.user_id, y.user_id) AS user_id
			,COALESCE(t.browser_type, y.browser_type) AS browser_type
			,CASE WHEN y.device_activity_datelist IS NULL THEN  ARRAY[t.date_active]
				WHEN t.date_active IS NULL THEN y.device_activity_datelist
			 	ELSE ARRAY[t.date_active] || y.device_activity_datelist 
			 END AS device_activity_datelist
			,COALESCE(t.date_active, y.date + INTERVAL '1 day') as DATE
		FROM today t 
		FULL OUTER JOIN yesterday y
		ON t.user_id = y.user_id and t.browser_type = y.browser_type
		;