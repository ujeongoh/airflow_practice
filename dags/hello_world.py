from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

dag = DAG(
    dag_id='HelloWorld',
    start_date=datetime(2022,6,8),
    catchup=False,
    tags=['example'],
    schedule='0 2 * * *'
)

def _print_hello():
    print("Hello!")
    return "Hello!"

def _print_goodbye():
    print("Good Bye!")
    return "Good Bye!"

print_hello = PythonOperator(
    task_id = 'print_hello',
    python_callable = _print_hello,
    dag = dag
)

print_goodbye = PythonOperator(
    task_id = 'print_goodbye',
    python_callable = _print_goodbye,
    dag = dag
)

print_hello >> print_goodbye