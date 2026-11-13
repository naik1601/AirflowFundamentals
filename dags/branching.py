from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def choose_branch(**context):
    """Return the task_id of the branch to follow."""
    logical_date = context["logical_date"]
    day_of_week = logical_date.day_of_week  # 0=Monday, 6=Sunday

    if day_of_week < 5:  # Monday to Friday
        return "full_pipeline"
    else:
        return "lightweight_pipeline"

with DAG(
    dag_id="branching_demo",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    branch = BranchPythonOperator(
        task_id="branch_decision",
        python_callable=choose_branch,
    )

    full = BashOperator(
        task_id="full_pipeline",
        bash_command="echo 'Running full ETL pipeline'",
    )

    lightweight = BashOperator(
        task_id="lightweight_pipeline",
        bash_command="echo 'Running lightweight checks only'",
    )

    branch >> [full, lightweight]
