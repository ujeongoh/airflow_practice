import requests
import json
from requests.auth import HTTPBasicAuth

with open('secrets.json') as f:
    secrets = json.load(f)

AIRFLOW_API_BASE_URL = 'http://localhost:8080/api/v1'
AIRFLOW_USERNAME = secrets['AIRFLOW_USERNAME']
AIRFLOW_PASSWORD = secrets['AIRFLOW_PASSWORD']

def get_all_dags():
    response = requests.get(
        f"{AIRFLOW_API_BASE_URL}/dags",
        auth=HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)                    
    )
    if response.status_code == 200:
        return response.json()["dags"]
    else:
        print(f"Error occurred while fetching DAGs: {response.text}")
        return None

def filter_not_paused(dags):
    return [dag for dag in dags if not dag["is_paused"]]

def main():
    """
    1. 사용자 인증하기
    2. 모든 dag 가져오기
    3. is_paused 가 False인 것들만 가져오기
    4. is_paused 가 False인 dag 정보를 json으로 출력하고 전체dag 수, is_paused 가 False인 dag 수 출력하기
    """
    all_dags = get_all_dags()
    if all_dags:
        not_paused_dags = filter_not_paused(all_dags)
        print(json.dumps(not_paused_dags, indent=2))
        print(f"Count of all dags: {len(all_dags)}")
        print(f"Count of not paused dags: {len(not_paused_dags)}")
        
if __name__ == "__main__":
    main()
