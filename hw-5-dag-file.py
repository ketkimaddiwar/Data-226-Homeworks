from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(symbol):
  """
   - return the last 90 days of the stock prices of symbol as a list of json strings
  """
  vantage_api_key = Variable.get('vantage_api_key')
  url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'
  r = requests.get(url)
  data = r.json()
  return data


@task
def transform(data):
  results = []   # empyt list for now to hold the 90 days of stock info (open, high, low, close, volume)
  for d in data["Time Series (Daily)"]:   # here d is a date: "YYYY-MM-DD"
    stock_info = data["Time Series (Daily)"][d]
    stock_info["date"] = d
    results.append(stock_info)
    if len(results) >= 90:
      break
  return results

@task
def load(cursor_obj, records, target_table):
   try:
     cursor_obj.execute("BEGIN;")
     cursor_obj.execute(f"""
     CREATE TABLE IF NOT EXISTS {target_table} (
       open float,close float,
       high float,low float,
       volume int,date date primary key UNIQUE,
       symbol char(3)
     )""")
     # load records
     for p in records: # we want records except the first one
         open = p['1. open']
         close = p['4. close']
         high = p['2. high']
         low = p['3. low']
         volume = p['5. volume']
         date = p['date']
         symbol='IBM'
         sql = f"INSERT INTO {target_table} (open, close, high, low, volume, date, symbol) SELECT '{open}', '{close}', '{high}', '{low}', '{volume}', '{date}', '{symbol}' WHERE NOT EXISTS ( SELECT 1 FROM {target_table} WHERE date = '{date}' AND symbol = '{symbol}')"
         # #First run
         cursor_obj.execute(sql)
     records = f"SELECT * FROM {target_table};"
     cursor_obj.execute(records)
     for row in cursor_obj:
       print(row)
     cursor_obj.execute("COMMIT;")
   except Exception as e:
       cursor_obj.execute("ROLLBACK;")
       print(e)
       raise e
   finally:
     cursor_obj.close()



with DAG(
    dag_id = 'hw-5-dag-run',
    start_date = datetime(2024,10,10),
    catchup=False,
    tags=['ETL'],
    schedule = '*/5 * * * *'
) as dag:
    target_table = "dev.raw_data.time_series"
    symbol = 'IBM'
    cur = return_snowflake_conn()
    data = extract(symbol)
    transformed_data = transform(data)
    load(cur, transformed_data, target_table)