from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

defaule_args ={
    'owner': 'justkacz',
    'retries': 5,
    'retry_delay':timedelta(minutes=1)
}


with DAG(
    dag_id='dag_cron',
    default_args=defaule_args,
    start_date=datetime(2022, 8, 19),
    schedule_interval='5 4 * 7 *' 
) as dag:
    task1=BashOperator(
        task_id='task1',
        bash_command='echo This is the dag with CRON expression'
    )
    task1