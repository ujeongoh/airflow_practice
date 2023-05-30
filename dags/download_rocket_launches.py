import json
import pathlib
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# DAG 인스턴스 생성 - 워크플로의 시작을 알린다.
dag = DAG(
    dag_id="download_rocket_launches", # 인터페이스에서 나타나는 DAG 이름
    description="Download rocket pictures of recently launched rockets.",
    start_date=airflow.utils.dates.days_ago(14), # DAG 실행 날짜
    schedule_interval="@daily", # DAG 실행간격
)

# 오퍼레이터(태스크) 생성

# 1. BashOperator 사용 - curl을 통해 결괏값을 받아온다
download_launches = BashOperator(
    task_id="download_launches", # 태스크 명으로 DAG의 노드에 표시되는 이름이다.
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",  # noqa: E501
    dag=dag,
)

# 2. PythonOperator 사용 - Operator의 콜러블에 전달할 함수명을 태스크 이름과 동일하게 지어주고 앞에 _를 붙인다.
#                         결괏값을 파싱하고 모든 로켓 사진을 다운로드한다.
def _get_pictures():
    # 디렉토리가 있는 지 확인한다.
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    # launches.jso으로 이미지를 다운로드
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")

get_pictures = PythonOperator(
    task_id="get_pictures", 
    python_callable=_get_pictures, # 콜러블 함수를 설정한다.
    dag=dag
)

# 3. BashOperator 사용 - curl을 통해 결괏값을 받아온다
notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag,
)

# 의존성 설정 - 태스크 실행 순서를 결정하여 의존성을 정의한다.
download_launches >> get_pictures >> notify