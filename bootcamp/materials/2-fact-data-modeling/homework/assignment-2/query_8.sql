
-- An incremental query that loads host_activity_reduced

   INSERT INTO host_activity_reduced (
    month_start,
    host,
    hit_array,
    unique_visitors
)
WITH yesterday AS (
    SELECT 
        month_start, 
        host, 
        hit_array, 
        unique_visitors 
    FROM host_activity_reduced
    WHERE month_start = DATE('2023-01-01')
),
today AS (
    SELECT
        host,
        DATE(event_time) AS event_date,
        COUNT(1) AS total_daily_visitors,
        COUNT(DISTINCT user_id) AS unique_daily_visitors
    FROM events
    WHERE user_id IS NOT NULL
      AND DATE(event_time) = DATE('2023-01-02')
    GROUP BY host, DATE(event_time)
),
aggregated_data AS (
    SELECT 
        date_trunc('month', COALESCE(t.event_date, y.month_start)) AS month_start,
        COALESCE(t.host, y.host) AS host,
        CASE 
            WHEN y.hit_array IS NULL THEN t.total_daily_visitors
            WHEN t.total_daily_visitors IS NULL THEN y.hit_array
            ELSE t.total_daily_visitors + y.hit_array
        END AS hit_array,
        CASE 
            WHEN y.unique_visitors IS NULL THEN ARRAY[t.unique_daily_visitors]
            WHEN t.unique_daily_visitors IS NULL THEN y.unique_visitors
            ELSE ARRAY_APPEND(y.unique_visitors, t.unique_daily_visitors)
        END AS unique_visitors
    FROM today t
    FULL OUTER JOIN yesterday y
    ON t.host = y.host
)
SELECT *
FROM aggregated_data
ON CONFLICT (month_start, host) 
DO UPDATE 
SET 
    hit_array = EXCLUDED.hit_array,
    unique_visitors = EXCLUDED.unique_visitors
;
