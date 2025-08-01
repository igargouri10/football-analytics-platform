# airflow/dags/football_data_ingestion_dag.py - CORRECT VERSION

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# Define the directory for your dbt project
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

with DAG(
    dag_id="football_data_ingestion",
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    tags=["football-data", "api", "dbt"],
    doc_md="""
        ## Football Data Ingestion DAG
        This DAG ingests match data from an API, loads it into S3,
        and then runs dbt to transform the data in the warehouse.
    """
) as dag:
    # Task to run the Python ingestion script
    ingest_script_task = BashOperator(
        task_id="run_ingestion_script",
        bash_command="python /opt/airflow/scripts/ingest_data.py",
    )

    # Task to run dbt models
    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Task to run dbt tests
    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
    )

    # This sets the order of execution for the tasks
    ingest_script_task >> dbt_run_task >> dbt_test_task