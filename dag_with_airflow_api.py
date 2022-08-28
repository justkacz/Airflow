from airflow.decorators import dag, task

from datetime import datetime, timedelta

default_args = {
    'owner':'justkacz',
    'retries':5,
    'retry_delay':timedelta(minutes=1)
}

# dag decorator:
@dag(
    dag_id='dag_with_taskflow_api', 
    default_args=default_args,
    start_date=datetime(2022, 8, 17),
    schedule_interval='@hourly'
)
def message():

    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name':'Justyna',
            'last_name':'Kaczmarek'
        }

    @task()
    def get_age():
        return 25

    @task()
    def greet(first_name, last_name, age):
        print(f'Hi {first_name} {last_name}! '
              f'you are {age} years old')
    
# the dependency of tasks will be detected automatically
    name_dict=get_name()
    age=get_age()
    greet(first_name=name_dict['first_name'], last_name=name_dict['last_name'], age=age)

greet_dag=message()