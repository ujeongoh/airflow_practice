FROM apache/airflow:2.6.1

USER airflow

COPY requirements.txt /

RUN pip install --no-cache-dir "apache-airflow==2.6.1" -r /requirements.txt