from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_taxi_raw_sink(t_env):
    table_name = 'taxi_raw_data'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            pickup_time STRING,
            dropoff_time STRING,
            pu_location_id INT,
            do_location_id INT,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            tip_amount DOUBLE
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
            tip_amount DOUBLE NULL
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'localhost:9092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'properties.group.id' = 'taxi-raw-consumer',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def diagnose_taxi_data():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    try:
        source_table = create_taxi_trips_source(t_env)
        sink_table = create_taxi_raw_sink(t_env)
        
        # Direct copy without windowing or timestamp parsing
        direct_sql = f"""
        INSERT INTO {sink_table}
        SELECT 
            lpep_pickup_datetime,
            lpep_dropoff_datetime,
            PULocationID,
            DOLocationID,
            passenger_count,
            trip_distance,
            tip_amount
        FROM {source_table}
        """
        
        print("Starting direct copy...")
        job_result = t_env.execute_sql(direct_sql)
        job_result.wait()
        print("Job completed successfully")
        
    except Exception as e:
        print("Error:", str(e))
        import traceback
        print(traceback.format_exc())

if __name__ == '__main__':
    diagnose_taxi_data()