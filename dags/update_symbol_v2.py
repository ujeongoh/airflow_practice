from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pandas import Timestamp

import yfinance as yf
import pandas as pd
import logging

def get_redshift_conn(autocommit=True):
    hook = PostgresHook(postgres_conn_id='aws_redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def _create_table(cur, schema, table, drop_first):
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table}
                (
                    date date,
                    "open" float,
                    high float,
                    low float,
                    close float,
                    volume bigint
                );            
                """)             

@task
def get_historical_prices(symbol):
    ticket = yf.Ticker(symbol)
    data = ticket.history() # DataFrame
    records = []
    
    for i, row in data.iterrows():
        date = i.strftime('%Y-%m-%d %H:%M:%S')
        records.append([date, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]])
    
    return records

@task
def load(schema, table, records):
    logging.info('Load started')
    cur = get_redshift_conn()
    
    try:
        cur.execute("BEGIN;")
        
        # 원본 테이블이 없으면 생성 - 테이블이 처음 한번 만들어질 때 필요한 코드
        _create_table(cur, schema, table, False)
        # 임시 테이블로 원본 테이블을 복사
        cur.execute(f"CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};")
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES ('{r[0]}', {r[1]}, {r[2]}, {r[3]}, {r[4]}, {r[5]});"
            print(sql)
            cur.execute(sql)
            
        # 원본 테이블 생성
        _create_table(cur, schema, table, True)
        # 임시 테이블 내용을 원본 테이블로 복사
        cur.execute(f"INSERT INTO {schema}.{table} SELECT DISTINCT * FROM t;")    
        cur.execute("COMMIT;")   # cur.execute("END;")
        
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    
    logging.info("Load done")      
    
with DAG(
    dag_id='update_symbol_v2',
    start_date=datetime(2023,6,7),
    catchup=False,
    tags=['API'],
    schedule='0 10 * * *'
) as dag:
    
    schema = "yujeong"
    table = "stock_info"
    results = get_historical_prices("AAPL")
    load(schema, table, results)