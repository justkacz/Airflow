import json
import pathlib
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import ssl

# resolves SSL: CERTIFICATE_VERIFY_FAILED error:
ssl._create_default_https_context = ssl._create_unverified_context


dag = DAG(
    dag_id="download_rocket_launches_v5",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)
download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -k -o /tmp/launches.json -L -4 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag,
)
def _get_pictures():
# Ensure directory exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
# Download all pictures in launches.json
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            response = requests.get(image_url)
            image_filename = image_url.split("/")[-1]
            target_file = f"/tmp/images/{image_filename}"
            with open(target_file, "wb") as f:
                f.write(response.content)
            print(f"Downloaded {image_url} to {target_file}")

get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag,
)
notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag,
)
download_launches >> get_pictures >> notify