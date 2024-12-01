

--Convert the device_activity_datelist column into a datelist_int column

WITH user_device AS (
		SELECT * 
		FROM user_devices_cumulated 
		WHERE date = DATE('2023-01-31') 
	),
	series AS (
		SELECT *
		FROM generate_series(DATE('2023-01-02'),DATE('2023-01-31'), INTERVAL '1 day') as series_date
	),
	place_holder_ints AS (	
		SELECT
			device_activity_datelist @> ARRAY[DATE(series_date)] AS is_active
			,CASE WHEN device_activity_datelist @> ARRAY[DATE(series_date)] THEN CAST(POW(2,32-(date-DATE(series_date))) AS BIGINT)
				ELSE 0
			END AS placeholder_int_value
			, *
		FROM user_device CROSS JOIN series 
	)
	SELECT user_id
			,browser_type
			,sum(placeholder_int_value)::BIGINT AS datelist_int 
	FROM place_holder_ints
	GROUP BY user_id,browser_type			
		;	
