

WITH deduped_events AS(
		SELECT 
			user_id
			,url
			,event_time
			,DATE(event_time) AS event_date
		FROM events e
		WHERE user_id IS NOT NULL
--		AND url IN ('/signup','/api/v1/login')
		group by user_id, url, event_time, DATE(event_time) 
	),
	self_join AS (
		SELECT 
			d1.user_id
			,d1.url
			,d2.url as destination_url
			,d1.event_time
			,d2.event_time
			,d1.event_date
		FROM deduped_events d1
		JOIN deduped_events d2
		ON d1.user_id = d2.user_id
		AND d1.event_date = d2.event_date
		AND d2.event_time > d1.event_time
--		WHERE d1.url = '/signup'	
	),
	user_level AS (
		SELECT 
			user_id
			,url
			,COUNT(1) AS number_of_hits
			,MAX(CASE WHEN destination_url ='/api/v1/login' THEN 1 ELSE 0 END) AS user_converted
		FROM self_join
		GROUP BY user_id,url
	)
	SELECT 
		url
		,SUM(number_of_hits) AS num_hits
		,SUM(user_converted) as num_converted
		,CAST(SUM(user_converted) AS REAL)/SUM(number_of_hits) AS pct_converted
--		,CAST(SUM(user_converted) AS REAL)/COUNT(1) AS pct_converted
	FROM user_level 
	GROUP BY url
	HAVING SUM(number_of_hits) > 500
	;

	
------------------------------------------------------------------------------------------------


CREATE TABLE device_hits_dashboard AS
WITH event_augmented AS(
		SELECT 
			COALESCE(d.os_type, 'UNKNOWN') AS os_type
			,COALESCE(d.device_type, 'UNKNOWN') AS device_type
			,COALESCE(d.browser_type, 'UNKNOWN') AS browser_type
			,e.user_id 
			,e.url 
		FROM events e 
		JOIN devices d 
		ON e.device_id = d.device_id 
	)
	SELECT
--		GROUPING(os_type)
--		,GROUPING(device_type)
--		,GROUPING(browser_type)
		CASE WHEN GROUPING(os_type) = 0 
				AND GROUPING(device_type) = 0
				AND GROUPING(browser_type) = 0
				THEN 'os_type__device_type__browser_type'
			WHEN GROUPING(os_type) = 0 THEN 'os_type'
			WHEN GROUPING(device_type) = 0 THEN 'device_type'
			WHEN GROUPING(browser_type) = 0 THEN 'browser_type'
		END AS aggretation_level	
		,COALESCE(os_type, '(overall)') AS os_type
		,COALESCE(device_type, '(overall)') AS device_type
		,COALESCE(browser_type, '(overall)') AS browser_type
		,COUNT(1) AS number_of_hits
	
	FROM event_augmented 
	GROUP BY GROUPING SETS(
		(browser_type,device_type,os_type)
		,(browser_type)
		,(os_type)
		,(device_type)
	)
	ORDER BY COUNT(1) DESC 
	

SELECT *  FROM device_hits_dashboard WHERE aggretation_level = 'os_type';

----------------------------------------------------------------------------
--------------------------- CUBE EXAMPLE------------------------------------


WITH event_augmented AS(
		SELECT 
			COALESCE(d.os_type, 'UNKNOWN') AS os_type
			,COALESCE(d.device_type, 'UNKNOWN') AS device_type
			,COALESCE(d.browser_type, 'UNKNOWN') AS browser_type
			,e.user_id 
			,e.url 
		FROM events e 
		JOIN devices d 
		ON e.device_id = d.device_id 
	)
	SELECT
--		GROUPING(os_type)
--		,GROUPING(device_type)
--		,GROUPING(browser_type)
		CASE WHEN GROUPING(os_type) = 0 
				AND GROUPING(device_type) = 0
				AND GROUPING(browser_type) = 0
				THEN 'os_type__device_type__browser_type'
			WHEN GROUPING(os_type) = 0 THEN 'os_type'
			WHEN GROUPING(device_type) = 0 THEN 'device_type'
			WHEN GROUPING(browser_type) = 0 THEN 'browser_type'
		END AS aggretation_level	
		,COALESCE(os_type, '(overall)') AS os_type
		,COALESCE(device_type, '(overall)') AS device_type
		,COALESCE(browser_type, '(overall)') AS browser_type
		,COUNT(1) AS number_of_hits
	
	FROM event_augmented 
	GROUP BY CUBE (browser_type,device_type,os_type)
	ORDER BY COUNT(1) DESC 
	

----------------------------------------------------------------------------
--------------------------- ROLLUP EXAMPLE------------------------------------


WITH event_augmented AS(
		SELECT 
			COALESCE(d.os_type, 'UNKNOWN') AS os_type
			,COALESCE(d.device_type, 'UNKNOWN') AS device_type
			,COALESCE(d.browser_type, 'UNKNOWN') AS browser_type
			,e.user_id 
			,e.url 
		FROM events e 
		JOIN devices d 
		ON e.device_id = d.device_id 
	)
	SELECT
--		GROUPING(os_type)
--		,GROUPING(device_type)
--		,GROUPING(browser_type)
		CASE WHEN GROUPING(os_type) = 0 
				AND GROUPING(device_type) = 0
				AND GROUPING(browser_type) = 0
				THEN 'os_type__device_type__browser_type'
			WHEN GROUPING(os_type) = 0 THEN 'os_type'
			WHEN GROUPING(device_type) = 0 THEN 'device_type'
			WHEN GROUPING(browser_type) = 0 THEN 'browser_type'
		END AS aggretation_level	
		,COALESCE(os_type, '(overall)') AS os_type
		,COALESCE(device_type, '(overall)') AS device_type
		,COALESCE(browser_type, '(overall)') AS browser_type
		,COUNT(1) AS number_of_hits
	
	FROM event_augmented 
	GROUP BY ROLLUP (browser_type,device_type,os_type)
	ORDER BY COUNT(1) DESC 
