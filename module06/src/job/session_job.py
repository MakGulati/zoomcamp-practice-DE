from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment, StreamTableEnvironment

def create_taxi_session_sink(t_env):
    table_name = 'taxi_session_data'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            pu_location_id INT,
            do_location_id INT,
            session_duration BIGINT,
            trip_count BIGINT,
            total_distance DOUBLE,
            total_passengers DOUBLE,
            PRIMARY KEY (session_start, pu_location_id, do_location_id) NOT ENFORCED
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

def create_taxi_trips_source(t_env):
    table_name = "taxi_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            PULocationID INT,
            DOLocationID INT,
            passenger_count DOUBLE NULL,
            trip_distance DOUBLE,
            tip_amount DOUBLE NULL,
            event_time AS TO_TIMESTAMP(lpep_dropoff_datetime),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def analyze_taxi_sessions():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(120 * 1000)  # Longer checkpointing for taxi data
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    # Configure idle timeout for long-running jobs
    t_env.get_config().set("table.exec.source.idle-timeout", "10 min")

    try:
        # Create Kafka source table and JDBC sink table
        source_table = create_taxi_trips_source(t_env)
        sink_table = create_taxi_session_sink(t_env)

        # Create a view with session windowing
        session_view_sql = f"""
        CREATE TEMPORARY VIEW taxi_sessions AS
        SELECT 
            PULocationID AS pu_location_id,
            DOLocationID AS do_location_id,
            SESSION_START(event_time, INTERVAL '5' MINUTES) AS session_start,
            SESSION_END(event_time, INTERVAL '5' MINUTES) AS session_end,
            TIMESTAMPDIFF(SECOND, 
                SESSION_START(event_time, INTERVAL '5' MINUTES),
                SESSION_END(event_time, INTERVAL '5' MINUTES)
            ) AS session_duration,
            COUNT(*) AS trip_count,
            SUM(trip_distance) AS total_distance,
            SUM(passenger_count) AS total_passengers
        FROM {source_table}
        GROUP BY 
            SESSION(event_time, INTERVAL '5' MINUTES),
            PULocationID,
            DOLocationID
        """
        
        t_env.execute_sql(session_view_sql)
        
        # Insert from view to sink table
        insert_sql = f"""
        INSERT INTO {sink_table}
        SELECT 
            session_start,
            session_end,
            pu_location_id,
            do_location_id,
            session_duration,
            trip_count,
            total_distance,
            total_passengers
        FROM taxi_sessions
        """
        
        # Execute the SQL
        print("Starting session window analysis...")
        job_result = t_env.execute_sql(insert_sql)
        print("Job submitted successfully")
        job_result.wait()
        print("Job completed successfully")

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))
        import traceback
        print(traceback.format_exc())

if __name__ == '__main__':
    analyze_taxi_sessions()