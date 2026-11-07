#ds = YYYY-MM-DD
#nodash = YYYYMMDD

from datetime import datetime
from pathlib import Path 

import pandas as pd 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


dag = DAG(
    dag_id="incremental_data_processing",
    # start_date=datetime(2026, 11, 6),
    # end_date=datetime(2024,6,7),
    # schedule_interval='@daily',
    catchup=False,
)


fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /opt/airflow/output/data/event && "
        "curl --fail --show-error "
        "-o /opt/airflow/output/data/event/events_{{ ds }}.json "
        "'http://events_api:5000/events?"
       "start_date=2026-10-01&end_date=2026-11-01'"
    ),
    dag=dag,
)
def _calculate_stats(input_path, output_path):
    Path(output_path).parent.mkdir(exist_ok=True)

    events= pd.read_json(input_path)
    stats = events.groupby(['date', 'user']).size().reset_index()

    stats.to_csv(output_path, index=False)

calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={
        "input_path": "/opt/airflow/output/data/event/events_{{ ds }}.json",
        "output_path": "/opt/airflow/output/data/event/output_{{ ds }}.csv",
    },
    dag=dag,
)

fetch_events >> calculate_stats