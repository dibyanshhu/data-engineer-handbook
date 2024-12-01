

--Create the table with the custom type as a column

CREATE TABLE user_devices_cumulated (
    user_id TEXT                                         -- Unique identifier for the user
    ,browser_type TEXT                                   -- brower type for each user 
    ,device_activity_datelist DATE[],                    -- Array of activity details per browser
    date DATE,                                           -- The date for the cumulative record
    PRIMARY KEY (user_id, browser_type, date)            -- Composite primary key ensures unique records per user and date
);