from airflow import DAG
from airflow.decorators import task
from datetime import datetime

@task
def print_hello():
    print("Hello!")
    return "Hello!"

@task
def print_goodbye():
    print("Good Bye!")
    return "Good Bye!"

with DAG(
    dag_id = 'HelloWorld_v2',
    start_date = datetime(2022,5,5),
    catchup=False,
    tags=['example'],
    schedule = '0 2 * * *'
) as dag:
    
    print_hello() >> print_goodbye()