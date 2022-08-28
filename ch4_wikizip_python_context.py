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
# with the context variables as key word arguments (in python function):

#def python_zip(data_interval_start):
def python_zip1(**context):
    #year, month, day, hour = data_interval_start.year, data_interval_start.month, data_interval_start.day, data_interval_start.hour,
    year, month, day, hour =  context['data_interval_start'].year, context['data_interval_start'].month, context['data_interval_start'].day, context['data_interval_start'].hour
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

with DAG(
    dag_id='wiki_zip_python_context',
    default_args=default_args,
    start_date=datetime(2022,8,23),
    schedule_interval='@hourly'
) as dag:
    download_zip1=PythonOperator(
        task_id='task_dwld_python1',
        python_callable=python_zip1
)

download_zip1