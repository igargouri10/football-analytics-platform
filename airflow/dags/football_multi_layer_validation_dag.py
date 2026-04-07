from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

AIRFLOW_ROOT = "/opt/airflow"
DBT_PROJECT_DIR = f"{AIRFLOW_ROOT}/dbt_project"
SCHEMA_DIR = f"{DBT_PROJECT_DIR}/models/marts"
GENERATED_DIR = f"{DBT_PROJECT_DIR}/generated_tests"

with DAG(
    dag_id="football_multi_layer_validation",
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["football-data", "multi-layer", "llm", "duckdb", "snowflake"],
    doc_md="""
    ## Football Multi-Layer Validation DAG

    End-to-end multi-layer workflow:

    1. Ingest raw football data into S3
    2. Restore manual schema baseline
    3. Build curated tables locally in DuckDB
    4. Generate LLM dbt tests from DuckDB schema/sample rows
    5. Merge generated tests into dbt schema
    6. Execute dbt tests locally with manual + LLM layers
    7. Migrate curated DuckDB outputs to Snowflake
    8. Validate DuckDB vs migrated Snowflake outputs
    """,
) as dag:

    reset_runtime_log = BashOperator(
        task_id="reset_runtime_log",
        bash_command=f"""
        set -euo pipefail
        python {AIRFLOW_ROOT}/experiments/reset_runtime_log.py
        """
    )
    
    ensure_schema_backup = BashOperator(
        task_id="ensure_schema_backup",
        bash_command=f"""
        set -euo pipefail
        if [ ! -f "{SCHEMA_DIR}/schema.manual_backup.yml.txt" ]; then
          cp "{SCHEMA_DIR}/schema.yml" "{SCHEMA_DIR}/schema.manual_backup.yml.txt"
        fi
        """
    )

    restore_manual_schema = BashOperator(
        task_id="restore_manual_schema",
        bash_command=f"""
        set -euo pipefail
        cp "{SCHEMA_DIR}/schema.manual_backup.yml.txt" "{SCHEMA_DIR}/schema.yml"
        mkdir -p "{GENERATED_DIR}"
        rm -f "{GENERATED_DIR}"/*_llm_tests.yml
        """
    )

    ingest_raw_data = BashOperator(
        task_id="ingest_raw_data",
        bash_command=f"""
        set -euo pipefail
        python {AIRFLOW_ROOT}/scripts/ingest_data.py
        """
    )

    dbt_run_duckdb_full_refresh = BashOperator(
        task_id="dbt_run_duckdb_full_refresh",
        bash_command=f"""
        set -euo pipefail
        python {AIRFLOW_ROOT}/experiments/run_dbt_duckdb_full_refresh.py
        """
    )

    generate_llm_tests = BashOperator(
        task_id="generate_llm_tests",
        bash_command=f"""
        set -euo pipefail
        cd {AIRFLOW_ROOT}
        python -m llm_tests.generate_dbt_tests
        """
    )

    merge_llm_tests = BashOperator(
        task_id="merge_llm_tests",
        bash_command=f"""
        set -euo pipefail
        cd {AIRFLOW_ROOT}
        python -m llm_tests.merge_generated_tests
        """
    )

    dbt_test_duckdb_with_llm = BashOperator(
        task_id="dbt_test_duckdb_with_llm",
        bash_command=f"""
        set -euo pipefail
        python {AIRFLOW_ROOT}/experiments/run_dbt_duckdb_test_with_llm.py
        """
    )

    migrate_duckdb_to_snowflake = BashOperator(
        task_id="migrate_duckdb_to_snowflake",
        bash_command=f"""
        set -euo pipefail
        python {AIRFLOW_ROOT}/experiments/migrate_duckdb_to_snowflake.py
        """
    )

    validate_duckdb_vs_migrated_snowflake = BashOperator(
        task_id="validate_duckdb_vs_migrated_snowflake",
        bash_command=f"""
        set -euo pipefail
        python {AIRFLOW_ROOT}/experiments/compare_duckdb_vs_migrated_snowflake.py
        """
    )

    run_multibatch_anomaly_experiment = BashOperator(
        task_id="run_multibatch_anomaly_experiment",
        bash_command=f"""
        set -euo pipefail
        python {AIRFLOW_ROOT}/experiments/run_multibatch_anomaly_experiment_with_logging.py
        """
    )

    run_generated_test_usefulness_audit = BashOperator(
        task_id="run_generated_test_usefulness_audit",
        bash_command=f"""
        set -euo pipefail
        python {AIRFLOW_ROOT}/experiments/run_generated_test_usefulness_audit_with_logging.py
        """
    )

    compile_research_summary = BashOperator(
        task_id="compile_research_summary",
        bash_command=f"""
        set -euo pipefail
        python {AIRFLOW_ROOT}/experiments/compile_research_summary.py
        """
    )

    (
        reset_runtime_log
        >> ensure_schema_backup
        >> restore_manual_schema
        >> ingest_raw_data
        >> dbt_run_duckdb_full_refresh
        >> generate_llm_tests
        >> merge_llm_tests
        >> dbt_test_duckdb_with_llm
        >> migrate_duckdb_to_snowflake
        >> validate_duckdb_vs_migrated_snowflake
        >> run_multibatch_anomaly_experiment
        >> run_generated_test_usefulness_audit
        >> compile_research_summary
    )