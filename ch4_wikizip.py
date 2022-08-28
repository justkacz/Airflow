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



#def downloadfile(**context):
#    url = context['templates_dict']['url'],
#    output_path=context['templates_dict']['output_path']
#
#    data = 'curl wget --no-check-certificate {url}'

with DAG(
    dag_id='wiki_zip',
    default_args=default_args,
    start_date=datetime(2022,8,23),
    schedule_interval='@hourly'
) as dag:
# using bash operator:
    download_zip= BashOperator(
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

# or the same result using python operator:
def python_zip(execution_date):
    year, month, day, hour, *_ = execution_date.timetuple(),
    url=(
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
        )

    path=os.path.join('/home/justkacz', 'zipfile')

    if os.path.isdir(path):
        request.urlretrieve(url, os.path.join(path, "wikipageviews.gz"))
    else:
        os.mkdir(path)
        request.urlretrieve(url, os.path.join(path, "wikipageviews.gz"))

download_zip2=PythonOperator(
    task_id='task_dwld2',
    python_callable=python_zip
)