from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import os.path

default_args={
    'owner':'justkacz',
    'retries':5,
    'retry_delay': timedelta(minutes=1)
}

def greet(name, age):
    print(f'The Python function has been called. My name is {name} '
    f'and I am {age} years old')

def where():
    print(os.getcwd())

with DAG(
    dag_id='python_dag',
    default_args=default_args,
    description='dag with a python operator',
    start_date=datetime(2022, 8, 16),
    schedule_interval='0 * * * *'
) as dag:
    task1=PythonOperator(
        task_id='pythontask1',
        python_callable=where,
        # op_kwargs provides values for the function parameters
        op_kwargs={'name':'Justyna', 'age':'30'}
    )

    task1