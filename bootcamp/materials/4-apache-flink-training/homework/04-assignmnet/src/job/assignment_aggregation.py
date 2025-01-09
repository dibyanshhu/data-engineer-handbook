"""
Kafka assignment for calculating the aggregation based on the specific url
to trigger this flink job it is also very necessary to setup the flink cluster using the docker file
and setup this flink job under that also add this job under the make file
"""
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import lit
from pyflink.table.window import Tumble


def create_processed_events_source_kafka(t_env) -> str:
    """
    Create the source table in Kafka that reads the processed web event data.
    Adjust this function according to your Kafka configuration.
    """
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    sink_ddl = f"""CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            host VARCHAR,
            url VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
             'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_ddl)

    return table_name


def create_sessionized_events_sink_postgres(t_env) -> str:
    """
    Create the PostgreSQL sink table where sessionized events will be written.
    Adjust this function according to your PostgreSQL configuration.
    """
    table_name = 'sessionized_events_sink_table'
    sink_ddl = f"""
    CREATE TABLE {table_name} (
            host VARCHAR,
            num_events INT,
            avg_events_per_session DOUBLE,
            unique_users INT
        )WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_sessionization():
    """
    This function sets up the Flink environment, creates necessary source and sink tables, 
    and performs sessionization based on IP and host. The sessionization uses a 5-minute gap.
    """
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # enable checkpointing every 10 seconds
    env.set_parallelism(3)  # set parallelism for better performance

    # Set up the table environment in streaming mode
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka source table
        source_table = create_processed_events_source_kafka(t_env)

        # Create PostgreSQL sink table for storing sessionized events
        session_sink_table = create_sessionized_events_sink_postgres(t_env)

        # SQL query for sessionization and metrics calculation
        sessionization_query = f"""
            INSERT INTO {session_sink_table}
            SELECT
                host,
                COUNT(*) AS num_events,
                AVG(events_in_session) AS avg_events_per_session,
                COUNT(DISTINCT ip) AS unique_users
            FROM (
                SELECT
                    window_start AS session_start,
                    window_end AS session_end,
                    ip,
                    host,
                    COUNT(*) AS events_in_session
                FROM TABLE(
                    SESSION(
                        TABLE {source_table},
                        DESCRIPTOR(event_timestamp),
                        INTERVAL '5' MINUTE
                    )
                )
                WHERE 
                    host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
                GROUP BY window_start, window_end, ip, host
            )
            GROUP BY host
        """
        
        # Execute the sessionization query
        t_env.execute_sql(sessionization_query).wait()

    except Exception as e:
        print("Sessionization job failed:", str(e))


if __name__ == '__main__':
    log_sessionization()
