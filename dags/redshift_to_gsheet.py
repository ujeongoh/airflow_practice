from airflow import DAG
from airflow.operators.python import PythonOperator

from plugins import gsheet
from plugins import slack
from datetime import datetime

redshift_conn_id = 'redshift_dev_db'

def update_gsheet(**context):
    sql = context['params']['sql']
    sheet_file_name = context['params']['sheet_file_name']
    sheet_gid = context['params']['sheet_gid']
    
    gsheet.update_sheet(sheet_file_name, sheet_gid, sql, redshift_conn_id)
    
with DAG(
    dag_id='sql_to_sheet',
    start_date = datetime(2022,6,18),
    catchup=False,
    tags=['gsheet'],
    schedule = '@once',
    default_args={
        'on_failure_callback': slack.on_failure_callback,
    }
) as dag:
    
    sheet_update = PythonOperator(
        task_id='update_sql_to_sheet',
        python_callable=update_gsheet,
        params={
            'sql': """
                SELECT 
                    date,
                    round AS nps
                FROM analytics.nps_summary
                ORDER BY 1
            """,
            'sheet_file_name': 'gsheet_airflow_practice',
            'sheet_gid': 'sql_to_gsheet',
        }
    )