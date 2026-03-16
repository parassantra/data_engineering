from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_trips_per_location_sink_postgres(t_env):
    table_name = 'trips_per_location'
    sink_ddl = f"""
    CREATE TABLE {table_name} (
        window_start TIMESTAMP(3),
        PULocationID INTEGER,
        num_trips BIGINT,
        PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/postgres',
        'table-name' = '{table_name}',
        'username' = 'postgres',
        'password' = 'postgres',
        'driver' = 'org.postgresql.Driver'
    );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_green_trips_source_kafka(t_env):
    table_name = "green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count FLOAT,
            trip_distance FLOAT,
            tip_amount FLOAT,
            total_amount FLOAT,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def log_processing():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Important: set to 1 for single partition topic
    env.enable_checkpointing(10 * 1000)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka table
        source_table = create_green_trips_source_kafka(t_env)
        postgres_sink = create_trips_per_location_sink_postgres(t_env)

        # Create tumbling window aggregation
        t_env.execute_sql(f"""
            INSERT INTO {postgres_sink}
            SELECT
                window_start,
                PULocationID,
                COUNT(*) as num_trips
            FROM TABLE(
                TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
            )
            GROUP BY window_start, PULocationID
        """).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()