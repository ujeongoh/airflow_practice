from datetime import datetime
import requests
import pandas as pd
import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import XCom

default_args = {
    'owner': 'ohyujeong',
    'start_date': datetime(2023, 6, 5),
}

dag = DAG(
    'reddit_api_kpop',
    default_args=default_args,
    schedule_interval='0 0 * * *'
)


def _process_json_to_csv(**context):
    headers = {
        'User-Agent': '(Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15)'
    }

    urls = [
        'https://www.reddit.com/r/kpop/hot.json?limit=100',
        'https://www.reddit.com/r/k-pop/hot.json?limit=100'
    ]
    df_list = []
    csv_filename = '/opt/airflow/outputs/reddit_data.csv'

    for url in urls:
        json_data = requests.get(url, headers=headers).json()
        kinds = [sub["kind"] for sub in json_data["data"]["children"]]
        titles = [sub["data"]["title"]
                  for sub in json_data["data"]["children"]]
        urls = [sub["data"]["url"] for sub in json_data["data"]["children"]]

        df = pd.DataFrame({"kind": kinds, "title": titles, "url": urls})
        df_list.append(df)

    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df.reset_index(inplace=True, drop=True)
    combined_df.to_csv(csv_filename, index=False)

    # csv 파일 경로를 xcom에 저장
    context['ti'].xcom_push(key='csv_filename', value=csv_filename)


def _upload_csv_to_s3(**context):
    # aws conn 정보 가져오기
    aws_conn = BaseHook.get_connection('aws_default')
    aws_access_key = aws_conn.login
    aws_secret_key = aws_conn.password

    # boto3 클라이언트 생성
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

    # xcom에 저장된 csv파일명 가져오기
    csv_filename = context['ti'].xcom_pull(key='csv_filename')

    # s3에 업로드
    bucket_name = 'yujeong-test-bucket'
    s3_file_name = 'reddit_data.csv'
    s3.upload_file(csv_filename, bucket_name, s3_file_name)


process_json_to_csv = PythonOperator(
    task_id='process_json_to_csv',
    python_callable=_process_json_to_csv,
    provide_context=True,
    dag=dag
)

upload_csv_to_s3 = PythonOperator(
    task_id='upload_csv_to_s3',
    python_callable=_upload_csv_to_s3,
    provide_context=True,
    dag=dag
)

process_json_to_csv >> upload_csv_to_s3
