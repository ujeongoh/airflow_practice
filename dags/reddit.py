from datetime import datetime
import requests
import pandas as pd
import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def _process_json_to_csv():
    headers = {
    'User-Agent' : '(Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15)'
    }

    urls = [
        'https://www.reddit.com/r/kpop/hot.json?limit=100', 
        'https://www.reddit.com/r/k-pop/hot.json?limit=100'
    ]
    df_list = []

    for url in urls:
        json_data = requests.get(url, headers=headers).json()
        kinds = [sub["kind"] for sub in json_data["data"]["children"]]
        titles = [sub["data"]["title"] for sub in json_data["data"]["children"]]
        urls = [sub["data"]["url"] for sub in json_data["data"]["children"]]
        
        df = pd.DataFrame({"kind": kinds, "title": titles, "url": urls})
        df_list.append(df)

    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df.reset_index(inplace = True, drop = True)
    combined_df.to_csv('reddit_data.csv', index = False)