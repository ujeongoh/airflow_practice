from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import logging

# 템플릿 참고
# https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
def get_redshift_conn(autocommit=True):
    hook = PostgresHook(postgres_conn_id='aws_redshift_learnde')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def get_countries_data():
    logging.info("Get countries data start")
    
    url = 'https://restcountries.com/v3.1/all?fields=name,population,area'
    response = requests.get(url)
    data = response.json()
    
    logging.info("Get countries data done")   
    return data

def _transform_data(**ctx):
    logging.info("Transform data start")
    
    data = ctx['ti'].xcom_pull(task_ids='get_countries_data')
    rows = []
    for d in data:
        name = d['name']['official'].replace("'", "''")
        population = d['population']
        area = d['area']
        
        rows.append([name, population, area])

    logging.info("Transform data done")
    return rows

def _load_data(**ctx):
    logging.info("Load data start")
    cur = get_redshift_conn()
    schema = ctx['params']['schema']
    table = ctx['params']['table']
    data = ctx['ti'].xcom_pull(task_ids='transform_data')
    
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table}(
                name varchar(200) primary key,
                population integer,
                area float
            );
        """)
        for d in data:
            sql = f"INSERT INTO {schema}.{table} VALUES ('{d[0]}','{d[1]}','{d[2]}')"
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    
    logging.info("Load data done")

with DAG(
    dag_id='restcountries',
    start_date=datetime(2023,6,7),
    catchup=False,
    tags=['API'],
    schedule='30 6 * * 6'
) as dag:
    params = {
        "schema" : "dbwjd090",
        "table" : "countries"
    }
    
    transform_data=PythonOperator(
        task_id='transform_data',
        python_callable=_transform_data,
        provide_context=True
    )
    
    load_data=PythonOperator(
        task_id='load_data',
        python_callable=_load_data,
        provide_context=True,
        op_kwargs={'params': params}
    )
    
    get_countries_data() >> transform_data >> load_data