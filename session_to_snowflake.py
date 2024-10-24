from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from airflow.utils.dates import days_ago

# Define default_args and DAG
default_args = {
    'owner': 'ketki',
    'depends_on_past': False,
    'start_date': datetime(2024,10,22),
    'retries': 1,
}
dag = DAG(
    'etl_flow_dag_real_time',
    default_args=default_args,
    schedule_interval='*/10 * * * *', 
)
#Create table 1
sql_command1 = """
CREATE OR REPLACE TABLE dev.raw_data.user_session_channel (
    userId int not NULL,
    sessionId varchar(32) primary key,
    channel varchar(32) default 'direct'  
);
"""
table_creation1 = SnowflakeOperator(
    task_id='create_table1_snowflake',
    snowflake_conn_id='snowflake_conn',  
    sql=sql_command1,
    dag=dag,
)

#Create table 2
sql_command2 = """
CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
    sessionId varchar(32) primary key,
    ts timestamp  
); 
"""
table_creation2 = SnowflakeOperator(
    task_id='create_table2_snowflake',
    snowflake_conn_id='snowflake_conn',  # Use your Snowflake connection ID
    sql=sql_command2,
    dag=dag,
)

#Create stage
sql_command3 = """
    CREATE OR REPLACE STAGE dev.raw_data.blob_stage
    url = 's3://s3-geospatial/readonly/'
	DIRECTORY = ( ENABLE = true );
"""
stage_creation = SnowflakeOperator(
    task_id='create_stage_snowflake',
    snowflake_conn_id='snowflake_conn',  # Use your Snowflake connection ID
    sql=sql_command3,
    dag=dag,
)

#Create file format
sql_command4 = """
 USE SCHEMA dev.raw_data;
 DROP FILE FORMAT IF EXISTS FILE_AF;
 CREATE FILE FORMAT FILE_AF
	TYPE=CSV
    SKIP_HEADER=1
    FIELD_DELIMITER=','
    TRIM_SPACE=TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY='"'
    REPLACE_INVALID_CHARACTERS=TRUE
    DATE_FORMAT=AUTO
    TIME_FORMAT=AUTO
    TIMESTAMP_FORMAT=AUTO;
"""

file_creation = SnowflakeOperator(
    task_id='create_file_format_snowflake',
    snowflake_conn_id='snowflake_conn',  # Use your Snowflake connection ID
    sql=sql_command4,
    dag=dag,
)

#Load data in table 1
sql_command5 = """
    USE SCHEMA dev.raw_data;
    COPY INTO dev.raw_data.user_session_channel
    FROM @dev.raw_data.blob_stage/user_session_channel.csv
    FILE_FORMAT = 'FILE_AF';
"""

load_into_snowflake1 = SnowflakeOperator(
    task_id='load_data_into_snowflake1',
    snowflake_conn_id='snowflake_conn',  # Use your Snowflake connection ID
    sql=sql_command5,
    dag=dag,
)

#Load data in table 2
sql_command6 = """
    USE SCHEMA dev.raw_data;
    COPY INTO dev.raw_data.session_timestamp
    FROM @dev.raw_data.blob_stage/session_timestamp.csv
    FILE_FORMAT = 'FILE_AF';
"""

load_into_snowflake2 = SnowflakeOperator(
    task_id='load_data_into_snowflake2',
    snowflake_conn_id='snowflake_conn',  # Use your Snowflake connection ID
    sql=sql_command6,
    dag=dag,
)
table_creation1>> table_creation2>> stage_creation >> file_creation >> load_into_snowflake1>> load_into_snowflake2