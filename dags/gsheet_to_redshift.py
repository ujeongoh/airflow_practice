from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta
from plugins import gsheet
from plugins import s3
from plugins import slack

import requests
import logging
import psycopg2
import json

def download_tab_in_gsheet(**context):
    url = context['params']['url']
    tab = context['params']['tab']
    table = context['params']['table']
    data_dir = Variable.get('DATA_DIR')
    
    gsheet.get_google_sheet_to_csv(
        url,
        tab,
        f'{data_dir}{table}.csv'
    )
        
def copy_to_s3(**context):
    table = context['params']['table']
    s3_key = context['params']['s3_key']      
    s3_conn_id = 'aws_conn_id'
    s3_bucket = 'grepp-data-engineering'
    data_dir = Variable.get('DATA_DIR')
    local_files_to_upload = [f'{data_dir}{table}.csv']
    replace = True
    
    s3.upload_to_s3(s3_conn_id, s3_bucket, s3_key, local_files_to_upload, replace)

with DAG(
    dag_id='gsheet_to_redshift',
    start_date=datetime(2021,11,27),
    schedule='0 0 * * *',
    max_active_runs=1,
    max_active_tasks=2,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'on_failure_callback': slack.on_failure_callback
    } 
) as dag:

    sheets = [
        {
            "url": "https://docs.google.com/spreadsheets/d/1sT6Z-TCIArQHxVPzz2Dsxbaxe3GB_Snpavqn1Uiu13A",
            "tab": "gsheet_to_redshift",
            "schema": "dbwjd090",
            "table": "spreadsheet_copy_testing"
        }
    ]   
    
    for sheet in sheets:
        
        table = sheet['table']
        schema = sheet['schema']
        
        download_tab_in_gsheet = PythonOperator(
            task_id=f"download_{table}_in_gsheet",
            python_callable=download_tab_in_gsheet,
            params=sheet
        )
        
        s3_key = f"{schema}_{table}"
        
        copy_to_s3 = PythonOperator(
            task_id = f"copy_{table}_to_s3",
            python_callable=copy_to_s3,
            params={
                'table': table,
                's3_key': s3_key
            }
        )
        
        run_copy_sql = S3ToRedshiftOperator(
            task_id=f"run_copy_sql_{table}",
            s3_bucket='grepp-data-engineering',
            s3_key = s3_key,
            schema = schema,
            table = table,
            copy_options = ['csv', 'IGNOREHEADER 1'],
            method = 'REPLACE',
            redshift_conn_id = 'redshift_dev_db',
            aws_conn_id = 'aws_conn_id'
        )
        
        download_tab_in_gsheet >> copy_to_s3 >> run_copy_sql