
-- DDL for hosts_cumulated table
CREATE TABLE hosts_cumulated(
		host TEXT
		,host_activity_datelist DATE[] --list of dates in the past where the user was active
		,date DATE --the current date for the user
		,PRIMARY KEY(host,date)
	);
