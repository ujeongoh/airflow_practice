from airflow import DAG
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from datetime import datetime
from datetime import timedelta

with DAG(
    dag_id = 'MySQL_to_Redshift_v2',
    start_date = datetime(2023,1,1), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 9 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
    schema = "dbwjd090"
    table = "nps"
    s3_bucket = "grepp-data-engineering"
    s3_key = f"{schema}-{table}"
    #sql = "SELECT * FROM prod.nps"
    sql = "SELECT * FROM prod.nps WHERE DATE(created_at) = DATE('{{ ds }}')"
    print(sql)
    
    mysql_to_s3 = SqlToS3Operator(
        task_id = "mysql_to_s3",
        query = sql,
        s3_bucket = s3_bucket,
        s3_key = s3_key,
        sql_conn_id = "mysql_conn_id",
        aws_conn_id = "aws_conn_id",
        verify = False,
        replace = True,
        pd_kwargs = {"index":False, "header":False} # Operator 내부에서 dataframe으로 처리하는데, 그때 설정할 세부 정보
    )
    
    s3_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_to_redshift_nps',
        s3_bucket = s3_bucket,
        s3_key = s3_key,
        schema = schema,
        table = table,
        copy_options = ['csv','IGNOREHEADER 1'],
        redshift_conn_id = 'aws_redshift_learnde',
        aws_conn_id = 'aws_conn_id',
        method = "UPSERT", # incremental 업데이트
        upsert_keys = ['id']
    )
    
    mysql_to_s3 >> s3_to_redshift