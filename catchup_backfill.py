# CATCHUP -> by default = True => the first run is at the start_date; when set to FALSE the first run is at the current time;
# allows to omit previous, historical runs (between start date and current date)
# the historical DAGs will not even show up in the DAG history.

from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

default_args = {
    'owner':'justkacz',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id='catchup_backfill_v2',
    default_args=default_args,
    start_date=datetime(2022, 8, 18),
    schedule_interval='@hourly',
    catchup=False # True is default
) as dag:
    task1=BashOperator(
        task_id='task1',
        bash_command='echo This is a task related to catchup or backfill'
    )

    task1