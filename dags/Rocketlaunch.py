
import json
import pathlib
import requests
import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from requests import exceptions as request_exception

dag = DAG(
    dag_id="Rocketlaunch",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["rocket", "learning"]
)

download_rocket_data = BashOperator(
    task_id="download_rocket_data",
    bash_command=(
        "curl -fLsS --retry 2 "
        "-o /tmp/rocket_data.json "
        "'https://ll.thespacedevs.com/2.2.0/launch/upcoming/?limit=5'"
    ),
    dag=dag
)


def get_pictures():

    image_dir = pathlib.Path("/tmp/images")
    image_dir.mkdir(parents=True, exist_ok=True)

    
    with open("/tmp/rocket_data.json", "r") as f:
        data = json.load(f)

   
    image_urls = [
        launch["image"]
        for launch in data["results"]
        if launch.get("image")
    ]

    print(f"Found {len(image_urls)} image URLs")

   
    for image_url in image_urls:
        try:
            response = requests.get(image_url, timeout=30)
            response.raise_for_status()

            image_name = image_url.split("?")[0].split("/")[-1]
            target_file = image_dir / image_name

            with open(target_file, "wb") as f:
                f.write(response.content)

            print(f"Downloaded: {target_file}")

        except requests.RequestException as e:
            print(f"Failed to download {image_url}: {e}")



get_pictures_task = PythonOperator(
    task_id="get_pictures",
    python_callable=get_pictures,
    dag=dag
)



notify_task = BashOperator(
    task_id="notify_task",
    bash_command=(
        'echo "Rocket launch data and images '
        'have been processed!"'
    ),
    dag=dag
)


download_rocket_data >> get_pictures_task >> notify_task
