from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from urllib import request
import os.path
import ssl

# resolves SSL: CERTIFICATE_VERIFY_FAILED error:
ssl._create_default_https_context = ssl._create_unverified_context

default_args = {
    'owner':'justkacz',
    'retries':5,
    'retry_delay':timedelta(minutes=1)
}

# using python operator:
# with the key word arguments (in Python Operator):
def python_zip2(year, month, day, hour, path):
    url=(
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
        )
    if os.path.isdir(path):
        request.urlretrieve(url, os.path.join(path, "wikipageviews.gz"))
    else:
        os.mkdir(path)
        request.urlretrieve(url, os.path.join(path, "wikipageviews.gz"))

with DAG(
    dag_id='wiki_zip_python_op_kwargs',
    default_args=default_args,
    start_date=datetime(2022,8,28),
    schedule_interval='@hourly'
) as dag:
    # task:
    download_zip2=PythonOperator(
        task_id='task_dwld_python2',
        python_callable=python_zip2,
        op_kwargs={
            'year': "{{ data_interval_start.year }}",
            'month': "{{ data_interval_start.month }}",
            'day': "{{ data_interval_start.day }}",
            'hour': "{{ data_interval_start.hour }}",
            'path': os.path.join('/home/justkacz', 'zipfile')
        }
    )   

download_zip2