from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta


default_args={
    'owner':'justkacz',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

with DAG(
    dag_id='my_first_dag',
    default_args=default_args,
    description='This is a description of the first dag.',
    start_date=datetime(2022, 8, 15, 2),
    schedule_interval='@daily'
) as dag:
    task1=BashOperator(
        task_id='first_task',
        bash_command='echo This is an example of the first task'
    )
task1

with DAG(
    dag_id='my_second_dag',
    default_args=default_args,
    description='This is a description of the first dag.',
    start_date=datetime(2022, 8, 15, 2),
    schedule_interval='@daily' # or CRON expr or timedelta if schedule should be set up at time interval
) as dag:
    task1=BashOperator(
        task_id='first_task',
        bash_command='echo This is an example of the first task'
    )
    task2=BashOperator(
        task_id='second_task',
        bash_command='echo This is an output of the second task'
    )

    task3=BashOperator(
        task_id='third_task',
        bash_command='echo This is a third task and will be running after the task1 and at the same time as task2'
    )

    #task1.set_downstream(task2)
    #task1.set_downstream(task3)

    task1.set_downstream([task2, task3])