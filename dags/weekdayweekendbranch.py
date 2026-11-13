from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def decide_pipeline(**context):
    day = context["logical_date"].day_of_week
    if day < 5:
        return "weekday_extract"
    return "weekend_check"

def extract_data(**context):
    print(f"Extracting full dataset for {context['ds']}")
    # Full extraction logic here

def run_quality_check(**context):
    print(f"Running lightweight quality checks for {context['ds']}")

with DAG(
    dag_id="weekday_weekend_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    branch = BranchPythonOperator(
        task_id="check_day_of_week",
        python_callable=decide_pipeline,
    )

    # --- Weekday path ---
    weekday_extract = PythonOperator(
        task_id="weekday_extract",
        python_callable=extract_data,
    )
    transform = BashOperator(
        task_id="transform",
        bash_command="echo 'Transforming data...'",
    )
    load = BashOperator(
        task_id="load",
        bash_command="echo 'Loading to warehouse...'",
    )

    # --- Weekend path ---
    weekend_check = PythonOperator(
        task_id="weekend_check",
        python_callable=run_quality_check,
    )

    # --- Common end ---
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    start >> branch
    branch >> weekday_extract >> transform >> load >> end
    branch >> weekend_check >> end
