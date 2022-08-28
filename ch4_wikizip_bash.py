from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta
from urllib import request
import os.path
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'justkacz',
    'retries':5,
    'retry_delay':timedelta(minutes=1)
}

with DAG(
    dag_id='wiki_zip_bash',
    default_args=default_args,
    start_date=datetime(2022,8,23),
    schedule_interval='@hourly'
) as dag:
    download_zip_bash= BashOperator(
        task_id='task_dwld',
        bash_command=(
            "curl -o /tmp/wikipageviews.gz "
            "https://dumps.wikimedia.org/other/pageviews/"
            "{{ execution_date.year }}/"
            "{{ execution_date.year }}-"
            "{{ '{:02}'.format(execution_date.month) }}/"
            "pageviews-{{ execution_date.year }}"
            "{{ '{:02}'.format(execution_date.month) }}"
            "{{ '{:02}'.format(execution_date.day) }}-"
            "{{ '{:02}'.format(execution_date.hour) }}0000.gz"
        )
    )

download_zip_bash