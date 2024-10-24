from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import logging

def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def run_ctas(table, select_sql, primary_key=None, additional_check=None):
    logging.info(table)
    logging.info(select_sql)

    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")
        sql = f"CREATE OR REPLACE TABLE {table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        # Primary key uniqueness check
        if primary_key is not None:
            pk_check_sql = f"SELECT {primary_key}, COUNT(1) AS cnt FROM {table} GROUP BY {primary_key} HAVING COUNT(1) > 1"
            cur.execute(pk_check_sql)
            result = cur.fetchall()
            if result:
                logging.error(f"Primary key uniqueness failed: {result}")
                raise Exception(f"Primary key uniqueness failed: {result}")

        # Additional check for duplicates
        if additional_check is not None:
            additional_check_sql = f"SELECT {additional_check}, COUNT(1) AS cnt FROM {table} GROUP BY {additional_check} HAVING COUNT(1) > 1"
            cur.execute(additional_check_sql)
            result = cur.fetchall()
            if result:
                logging.error(f"Duplicate records found based on additional check: {result}")
                raise Exception(f"Duplicate records found based on additional check: {result}")

        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to execute SQL. Completed ROLLBACK!')
        raise

with DAG(
    dag_id='BuildELT_CTAS',
    start_date=datetime(2024, 10, 23),
    catchup=False,
    tags=['ELT'],
    schedule='*/5 * * * *'
) as dag:

    table = "dev.analytics.session_summary"
    select_sql = """SELECT u.*, s.ts
    FROM dev.raw_data.user_session_channel u
    JOIN dev.raw_data.session_timestamp s ON u.sessionId=s.sessionId
    """

    # Call run_ctas with an additional check for duplicate records
    run_ctas(table, select_sql, primary_key='sessionId', additional_check='userId')  # Example using 'userId' for additional check
