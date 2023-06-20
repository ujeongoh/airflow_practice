from airflow.macros import *

from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

import logging
from glob import glob
"""
Build a summary table under analytics schema in Redshift
- Check the input tables readiness first
- Run the SQL using a temp table
- Before swapping, check the size of the temp table
- Finally swap
"""

def load_all_jsons_into_list(path_to_json): 
    """
    주어진 경로에서 모든 파이썬 파일을 읽고, 내용을 dict형으로 리스트에 추가한다.

    :param path_to_json: 파이썬 파일이 포함된 디렉토리의 경로
    :return: 사전의 리스트
    """
    configs = []
    
    # Loop through each Python file in the directory
    for f_name in glob(f'{path_to_json}/*.py'):
        with open(f_name) as f:
            dict_text = f.read()
            try:
                # Try to evaluate the content as a dictionary
                dict = eval(dict_text)
            except Exception as e:
                logging.info(str(e))
            else: 
                configs.append(dict)
    return configs

def find(table_name, table_confs):
    """
    설정 리스트(table_confs)에서 특정 테이블(table_name)의 설정을 검색하고, 있다면 그 설정을 반환한다.
    
    :param table_name: 설정이 필요한 테이블의 이름.
    :param table_confs: 각 테이블의 설정을 나타내는 딕셔너리의 리스트.
    :return: 지정된 테이블의 설정을 딕셔너리 형태로 반환한다. 일치하는 테이블이 없을 경우 None을 반환한다.
    """
    for table_conf in table_confs:
        if table_conf.get("table") == table_name:
            return table_conf
    return None

def build_summary_table(dag_root_path, dag, tables_load, redshift_conn_id, start_task=None):
    """
    build_summary_table 함수는 주어진 테이블들에 대해 요약 테이블을 생성하는 역할을 수행한다. 
    Airflow DAG와 연계하여 각 테이블에 대해 RedshiftSummaryOperator를 생성하고, 그 연산자를 통해 테이블 요약 작업을 실행한다.
    
    :param dag_root_path: DAG 파일의 루트 경로. 이 경로 안에는 테이블 구성을 저장하는 JSON 파일이 있어야 한다.
    :param dag: Airflow DAG 객체. 이 객체 내부에 요약 작업을 추가한다.
    :param tables_load: 요약 테이블을 생성할 테이블 이름 리스트.
    :param redshift_conn_id: Redshift 데이터베이스에 연결하는데 사용되는 Airflow connection ID.
    :param start_task: DAG에서 시작 태스크. 이 태스크 뒤에 요약 태스크들이 추가된다. 기본값은 None.
    :return: 마지막 요약 태스크. 이 태스크 뒤에 다른 태스크를 추가할 수 있다.
    """
    logging.info(dag_root_path) # 로깅에 dag_root_path 정보 추가
    table_confs = load_all_jsons_into_list(f'{dag_root_path}/config/') # JSON 파일들을 읽어서 설정 리스트 생성
    
    # 시작 태스크 지정. 만약 start_task가 주어지지 않았다면, 이전 태스크(prev_task)는 None이 된다.
    if start_task:
        prev_task = start_task
    else:
        prev_task = None
        
    # tables_load 리스트를 순회하며 각 테이블에 대한 요약 작업 생성
    for table_name in tables_load:
        table = find(table_name, table_confs) # 테이블 구성 찾기
        logging.info(table.get('input_check'))
        logging.info(table.get('output_check'))
        
        
        # RedshiftSummaryOperator 생성
        summarizer = RedshiftSummaryOperator(
            table=table['table'],
            schema=table['schema'],
            redshift_conn_id=redshift_conn_id,
            input_check=table.get('input_check'),
            main_sql=table["main_sql"],
            output_check=table.get('output_check'),
            overwrite=table.get("overwrite", True),
            post_sql=table.get("post_sql"),
            pre_sql=table.get("pre_sql"),
            attributes=table.get("attributes"),
            dag=dag,
            task_id=f"anayltics__{table['table']}"
        )
        
        # 이전 태스크가 존재한다면, 해당 태스크 뒤에 요약 태스크를 추가한다.
        if prev_task:
            prev_task >> summarizer
        prev_task = summarizer # 마지막으로 추가된 요약 태스크를 이전 태스크로 업데이트
    return prev_task # 마지막 요약 태스크 반환

    
def redshift_sql_function(*context):
    """
     Redshift 데이터베이스에서 SQL 쿼리를 실행한다.
    하나 이상의 컨텍스트 딕셔너리를 받아야 한다. 각 딕셔너리에는 실행할 SQL 쿼리와 Redshift 데이터베이스에 대한 연결 ID가 포함되어야 한다.
    SQL 쿼리를 로깅하고, 제공된 연결 ID를 사용하여 Redshift 데이터베이스에 연결한 후 쿼리를 실행한다.

    :param context: 하나 이상의 딕셔너리로, 각각 두 개의 키-값 쌍을 포함한다:
    - 'sql': 실행할 SQL 쿼리를 나타내는 문자열.
    - 'redshift_conn_id': Redshift 데이터베이스에 대한 연결 ID를 나타내는 문자열.
    
    """
    sql = context['params']['sql']
    logging.info(sql)
    hook = PostgresHook(postgres_conn_id=context['params']['redshift_conn_id']) 
    hook.run(sql, True)   
        
class RedshiftSummaryOperator(PythonOperator):
    # apply_defaults 데코레이터를 사용하여 Airflow의 BaseOperator에서 제공하는 인자들을 자동으로 상속받는다.
    
    @apply_defaults
    def __init__(self,
                 schema,  # 스키마 이름
                 table,  # 테이블 이름
                 redshift_conn_id,  # Redshift 연결 ID
                 overwrite,  # 덮어쓰기 여부
                 input_check,  # 입력 유효성 검사
                 main_sql,  # 메인 SQL 쿼리
                 output_check,  # 출력 유효성 검사
                 params={},  # 추가 매개변수
                 pre_sql="",  # 메인 SQL 쿼리 이전에 실행할 SQL 쿼리
                 post_sql="",  # 메인 SQL 쿼리 이후에 실행할 SQL 쿼리
                 attributes="",  # 생성될 테이블의 속성
                 *args, 
                 **kwargs
                 ):
        logging.info(input_check)
        logging.info(output_check)
        # 클래스 멤버 변수들을 초기화한다.
        self.schema = schema,
        self.table = table,
        self.redshift_conn_id = redshift_conn_id,
        self.input_check = input_check,
        self.main_sql = main_sql,
        self.output_check = output_check
        
        # pre_sql이 주어졌다면 main_sql 앞에 추가한다.
        # 이때, pre_sql의 마지막에 세미콜론이 없다면 추가한다.
        if pre_sql:
            main_sql = pre_sql
            if not main_sql.endswith(';'):
                main_sql += ';'
        else:
            main_sql = ''
            
        # 메인 쿼리를 작성한다. 임시 테이블을 만들고 메인 쿼리를 실행한 결과를 저장한다.
        main_sql += f'''
            DROP TABLE IF EXISTS {self.schema}.temp_{self.table};       
            CREATE TABLE {self.schema}.temp_{self.table} {attributes} AS {self.main_sql}
        '''
        
        # post_sql이 주어지면 self.post_sql에 저장한다.
        # post_sql이 없다면 self.post_sql을 빈 문자열로 설정한다.
        if post_sql:
            self.post_sql = post_sql.format(schema=self.schema, table=self.table)
        else:
            self.post_sql = ''
            
        # 상위 클래스의 생성자를 호출하여 Operator를 초기화한다.
        # 여기서 redshift_sql_function이 python_callable로 전달되고, 매개변수들이 params에 전달된다.
        super(RedshiftSummaryOperator, self).__init__(
            python_callable=redshift_sql_function,
            params={
                'sql': main_sql,
                'overwrite': overwrite,
                'redshift_conn_id': self.redshift_conn_id
            },
            provide_context=True,
            *args, 
            **kwargs
        )
        
    # swap 메소드는 임시 테이블을 실제 테이블로 대체하고, 사용자 그룹에 SELECT 권한을 부여하는 역할을 한다.
    def swap(self):
        users = 'analytics_users'
        sql = f"""
            BEGIN;
                DROP TABLE IF EXISTS {self.schema}.{self.table} CASCADE;
                ALTER TABLE {self.schema}.temp_{self.table} RENAME TO {self.table};
                GRANT SELECT ON TABLE {self.schema}.{self.table} TO GROUP {users};
            END;
        """
        self.hook.run(sql, True)
    
    # execute 메소드는 Operator가 수행해야 하는 주요 동작을 정의한다.
    # 입력 유효성 검사와 출력 유효성 검사를 수행한다.
    # 검사에 실패하면 AirflowException을 발생시킨다.
    def execute(self, context):
        # Redshift 연결을 초기화한다.
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        logging.info(self.input_check)
        logging.info(self.output_check)
        
        if self.input_check:
            # 입력 유효성 검사를 수행한다.
            for item in self.input_check:
                logging.info(item)
                item = item[0]
                result = self.hook.get_first(item['sql'])
                if result:
                    (cnt,) = result
                    if int(cnt) < int(item['count']):
                        raise AirflowException(f"Input Validation Failed for {str(item['sql'])}")
            
        # 메인 SQL 쿼리를 실행한다.
        return_value = super(RedshiftSummaryOperator, self).execute(context)
        
        if self.output_check:
            # 출력 유효성 검사를 수행한다.
            for item in self.output_check:
                logging.info(item['sql'])
                result = self.hook.get_first(item["sql"].format(schema=self.schema, table=self.table))
                if result:
                    (cnt,) = result
                    if item.get('op') == 'eq':
                        if int(cnt) != int(item["count"]):
                            raise AirflowException(
                                f"Output Validation of 'eq' Failed for {item['sql']}: {cnt} vs. {item['count']}"
                            )
                    else:
                        if cnt < item["count"]:
                            raise AirflowException(
                                f"Output Validation Failed for {item['sql']}: {cnt} vs. {item['count']}"
                            )
        
        # 임시 테이블을 실제 테이블로 대체한다.
        self.swap()
        
        # post_sql이 있다면 실행한다.
        if self.post_sql:
            self.hook.run(self.post_sql, True)

        # 메인 SQL 쿼리의 실행 결과를 반환한다.
        return return_value

