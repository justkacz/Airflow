from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

def get_name(ti):
    ti.xcom_push(key='first_name', value='Justyna')
    ti.xcom_push(key='last_name', value='Kaczmarek')

def get_age(ti):
    ti.xcom_push(key='age', value=100)

def greet(ti):
    first_name=ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name=ti.xcom_pull(task_ids='get_name', key='last_name')
    age=ti.xcom_pull(task_ids='get_age', key='age')
    print(f"print name: {first_name} {last_name} and age: {age}")


default_args={
    'owner':'justkacz',
    'retries':5,
    'retry_delay':timedelta(minutes=1)
}

with DAG(
    dag_id='xcom_py_push2',
    default_args=default_args,
    description='This is an example of xcom dag.',
    start_date=datetime(2022,8,15),
    schedule_interval='@hourly'
) as dag:
    task1=PythonOperator(
        task_id='greet',
        python_callable=greet,
        #op_kwargs={'age':'30'}
    )
    
    task2=PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task3=PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

    [task2, task3] >> task1
