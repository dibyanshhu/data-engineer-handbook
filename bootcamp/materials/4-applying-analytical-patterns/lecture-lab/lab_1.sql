-- CREATE TABLE users_growth_accounting (
--     user_id TEXT,
--     first_active_date DATE,
--     last_active_date DATE,
--     daily_active_state TEXT,
--     weekly_active_state TEXT,
--     dates_active DATE[],
--     date DATE,
--     PRIMARY KEY (user_id, date)
-- );
 
SELECT
       date - first_active_date AS days_since_first_active,
       CAST(COUNT(CASE
           WHEN daily_active_state
                    IN ('Retained', 'Resurrected', 'New') THEN 1 END) AS REAL)/COUNT(1) as pct_active,
       COUNT(1) FROM users_growth_accounting
GROUP BY date - first_active_date
ORDER BY date - first_active_date;


--INSERT INTO users_growth_accounting 
--WITH yeaterday AS (
--		SELECT 
--			*
--		FROM users_growth_accounting
--		WHERE date = DATE('2023-01-08')
--		
--	),
--	today AS(
--		SELECT 
--			user_id::TEXT AS user_id  
--			,DATE_TRUNC('day', event_time::timestamp) AS today_date
--			,COUNT(1)
--		FROM events e 
--		WHERE DATE_TRUNC('day', event_time::timestamp) =  DATE('2023-01-09')
--		AND user_id IS NOT NULL
--		GROUP BY user_id, DATE_TRUNC('day', event_time::timestamp)
--	)
--	SELECT 
--		COALESCE(t.user_id, y.user_id) AS user_id 
--		,COALESCE(y.first_active_date,t.today_date) AS first_active_date
--		,COALESCE(t.today_date,y.last_active_date) AS last_active_date
--		,CASE 
--			WHEN y.user_id IS NULL THEN 'New'
--			WHEN y.last_active_date = t.today_date - INTERVAL '1 day' THEN 'Retained'
--			WHEN y.last_active_date < t.today_date - INTERVAL '1 day' THEN 'Resurrected'
--			WHEN t.today_date IS NULL AND y.last_active_date = y.date THEN 'Churned'
--			ELSE 'Stale' 
--		END AS daily_active_state
--		,CASE 
--			WHEN y.user_id IS NULL THEN 'New'
--			WHEN y.last_active_date < t.today_date - INTERVAL '7 day' THEN 'Resurrected'	
--			WHEN y.last_active_date > y.date - INTERVAL '7 day' THEN 'Retained'
--			WHEN t.today_date IS NULL AND y.last_active_date = y.date - INTERVAL '7 day' THEN 'Churned'
--			ELSE 'Stale'
--		END AS weekly_active_state
--		,COALESCE(y.dates_active,ARRAY []::DATE[])
--                    || CASE
--                           WHEN t.user_id IS NOT NULL THEN ARRAY [t.today_date]
--                           ELSE ARRAY []::DATE[]
--                           END AS date_list
--		,COALESCE(t.today_date, y.date + Interval '1 day') as date
--		
-- 	FROM today as t 
--	FULL OUTER JOIN yeaterday as y 
--	ON t.user_id = y.user_id ;
	
