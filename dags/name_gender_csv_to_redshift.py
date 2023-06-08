from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime
from datetime import timedelta
import requests
import logging
import psycopg2

def get_redshift_conn(autocommit=True):
    hook = PostgresHook(postgres_conn_id='aws_redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

# Task

@task
def extract(url):
    logging.info("Extract started")
    f = requests.get(url)
    logging.info("Extract done")
    return f.text

@task
def transform(text):
    logging.info("Transform started")
    lines = text.strip().split("\n")[1:]
    records = []
    
    for l in lines:
        name, gender = l.split(",")
        records.append([name, gender])
    
    logging.info("Transform done")
    return records

@task
def load(schema, table, records):
    logging.info("Load started")
    cur = get_redshift_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.{table};")
        
        for r in records:
            name = r[0]
            gender = r[1]
            print(f"{name}-{gender}")
            sql = f"INSERT INTO {schema}.{table} VALUES ('{name}', '{gender}');"
            cur.execute(sql)
        
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
    logging.info("Load done")
    
with DAG(
    dag_id='name_gender',
    start_date=datetime(2023,6,6),
    schedule='0 2 * * *',
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay':timedelta(minutes=3),
    }
) as dag:
    url = Variable.get("csv_url")
    schema = 'yujeong'
    table = 'name_gender'
    
    # 이전 task의 출력이 다음 task의 입력으로 전달
    # lines = extract(url) >> transform >> load(schema, table)
    
    lines = transform(extract(url))
    load(schema, table, lines)