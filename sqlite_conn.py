from logging.handlers import DatagramHandler
from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

from datetime  import datetime, timedelta

default_args={
    'owner':'justkacz',
    'retries':5,
    'retry_delay':timedelta(minutes=1)
}

with DAG(
    dag_id='dag_sqlite',
    default_args=default_args,
    start_date=datetime(2022, 8, 20),
    schedule_interval='0 0 * * *'
) as dag:
    task1=SqliteOperator(
        task_id='task_sqlite',
        sqlite_conn_id='airflow_db',
        sql="""
            create table if not exists dag_runs(
                dag_id integer primary key, 
                dt date
            )
        """
    )
    task1
