/* DDL for creating the Postgres table */
CREATE TABLE sessionized_events_sink_table (
            host VARCHAR,
            num_events INT,
            avg_events_per_session DOUBLE,
            unique_users INT
        );


--1. Calculate the average number of web events per session for each host
WITH agv_num AS (
    SELECT 
        host
        ,AVG(avg_events_per_session) AS avg_events_per_host
    FROM sessionized_events_sink_table
    GROUP BY host
    )
    SELECT  * FROM agv_num;

--2. Compare total number of events 
WITH avg_sum AS(
    SELECT 
        host
        ,SUM(num_events) AS total_events
    FROM sessionized_events_sink_table
    GROUP BY host
    )
    SELECT * FROM avg_sum;

/*in order to execute this .sql file postgres needs to up and running
and data from flink job should be continously following into the sessionized_events_sink_table table
post that script 1 gives What is the average number of web events of a session from a user on Tech Creator
and 2. gives us the total number to event happeing within the give time window with respenct to 
the urls (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
*/
