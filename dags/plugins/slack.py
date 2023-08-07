from airflow.models import Variable
import os
from airflow import settings
import logging
import requests
from collections import deque

def tail(file_path, n=20):
    """
    Returns the last `n` lines from the file defined at `file_path`.

    :param file_path: the path of the file.
    :param n: the number of lines to return.
    :returns: the last `n` lines as a list of strings.
    """
    try:
        with open(file_path, 'r') as f:
            return deque(f, n)
    except FileNotFoundError:
        return ["Log file not found."]
    
def on_failure_callback(context):
    """
    https://airflow.apache.org/_modules/airflow/operators/slack_operator.html
    Define the callback to post on Slack if a failure is detected in the Workflow
    :return: operator.execute
    """
    
    # 로그 가져올 수 있도록 추가
    dag_id = context['dag'].dag_id
    run_id = context['dag_run'].run_id
    task_id = context['task'].task_id
    try_number = context['task_instance'].try_number
    
    log_path = os.path.join(
        settings.AIRFLOW_HOME, 
        "logs", 
        f'dag_id={dag_id}', 
        f'run_id={run_id}', 
        f'task_id={task_id}',
        f'attempt={str(int(try_number) - 1)}.log'
    )

    log_text = '\n'.join(tail(log_path, 10))
    
    text = str(context['task_instance'])
    text += f"``` {str(context.get('exception'))} \n +---------------------------------------------------+ \n {log_text} ```"
    send_message_to_a_slack_channel(text, ":cry:")


# def send_message_to_a_slack_channel(message, emoji, channel, access_token):
def send_message_to_a_slack_channel(message, emoji):
    # url = "https://slack.com/api/chat.postMessage"
    url = f"https://hooks.slack.com/services/{Variable.get('slack_url')}"
    headers = {
        'content-type': 'application/json',
    }
    data = { "username": "Data GOD", "text": message, "icon_emoji": emoji }
    r = requests.post(url, json=data, headers=headers)
    return r