# airflow/dags/football_data_ingestion_dag.py

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="football_data_ingestion",
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),
    schedule_interval="@daily",  # Run once a day
    catchup=False,
    tags=["football-data", "api"],
    doc_md="""
        ## Football Data Ingestion DAG
        This DAG ingests match data from an API and loads it into S3.
    """
) as dag:
    ingest_script_task = BashOperator(
        task_id="run_ingestion_script",
        # This is the command that will be executed inside the Airflow container
        bash_command="python /opt/airflow/scripts/ingest_data.py",
    )