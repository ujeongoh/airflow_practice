from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging


def get_redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

def _exec_sql(**ctx):
    schema = ctx['params']['schema']
    table = ctx['params']['table']
    select_sql = ctx['params']['sql']
    
    logging.info(schema)
    logging.info(table)
    logging.info(select_sql)
    
    cur = get_redshift_connection()
    
    # 임시 테이블 생성 후 CTAS로 원본테이블 데이터 적재 
    sql = f"""
        DROP TABLE IF EXISTS {schema}.temp_{table};
        CREATE TABLE {schema}.temp_{table} AS
    """
    sql += select_sql
    cur.execute(sql)
    
    # 원본테이블의 데이터 유무 확인
    cur.execute(f"SELECT COUNT(1) FROM {schema}.temp_{table}")
    count = cur.fetchone()[0]
    if count == 0:
        raise ValueError(f"{schema}.{table} didn't have any record")
    
    try:
        # 원본 테이블 삭제 후 임시테이블 이름을 원본 테이블 이름으로 변경
        sql = f"""
            DROP TABLE IF EXISTS {schema}.{table};
            ALTER TABLE {schema}.temp_{table} RENAME to {table};
            COMMIT;
        """
        logging.info(sql)
        cur.execute(sql)
        
    except:
        cur.execute("ROLLBACK;")
        logging.error('Failed to sql. Completed ROLLBACK.')
        raise
    
with DAG(
    dag_id="build_summary",
    start_date=datetime(2021,12,10),
    schedule='@once',
    catchup=False
) as dag:
    exec_sql = PythonOperator(
        task_id='mau_summary',
        python_callable=_exec_sql,
        params={
            'schema':'dbwjd090',
            'table':'mau_summary',
            'sql': """
                SELECT
                    TO_CHAR(A.ts, 'YYYY-MM-DD') AS month,
                    COUNT(DISTINCT B.userid) AS mau
                FROM raw_data.session_timestamp A
                JOIN raw_data.user_session_channel B ON A.sessionid = B.sessionid
                GROUP BY 1;
            """
        }
    )