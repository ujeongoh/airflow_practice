from airflow import DAG
from airflow.macros import datetime
from airflow.macros import timedelta
import os

from plugins import redshift_summary
from plugins import slack

with DAG(
    dag_id='build_summary_v2',
    schedule_interval="25 13 * * *",
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    start_date=datetime(2021, 9, 17),
    default_args= {
        # 'on_failure_callback': slack.on_failure_callback,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:
    # this should be listed in dependency order (all in analytics)
    tables_load = [
        'nps_summary', 'mau_summary', 'channel_summary'
    ]

    dag_root_path = os.path.dirname(os.path.abspath(__file__))
    redshift_summary.build_summary_table(dag_root_path, dag, tables_load, "redshift_dev_db")