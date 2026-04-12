# Football Analytics Platform

A reproducible, research-oriented ELT and data-quality validation platform for football analytics.

This repository implements a multi-layer testing workflow for cloud-native ELT pipelines using **Apache Airflow**, **dbt**, **DuckDB**, **Snowflake**, **Amazon S3**, and **OpenAI GPT-4.1-mini**.

## Repository structure

```text
airflow/                     # DAGs and orchestration support
dbt_project/                 # dbt models, schemas, generated tests, schema versions
llm_tests/                   # LLM prompts, generation, merge logic
experiments/                 # anomaly evaluation, usefulness audit, runtime logging, summaries
dashboard/                   # optional dashboard layer
docs/                        # setup and reproducibility notes
```

## Main research result

In the final comparator-based evaluation:
- weak manual-only baseline: **7 / 16**
- manual-expanded comparator: **16 / 16**
- manual-plus-LLM: **16 / 16**

## Quick start

### Clone
```bash
git clone https://github.com/igargouri10/football-analytics-platform.git
cd football-analytics-platform
```

### Create virtual environment
**Windows PowerShell**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

**macOS / Linux**
```bash
python -m venv .venv
source .venv/bin/activate
```

### Install dependencies
```bash
pip install -r requirements.txt
```

### Configure environment
Copy `.env.example` to `.env` and fill in your values.

### Configure dbt
Copy `.dbt/profiles.example.yml` into your local dbt profiles directory and customize it.

### Run local workflow
```bash
dbt run --project-dir dbt_project --profiles-dir "$HOME/.dbt"
dbt test --project-dir dbt_project --profiles-dir "$HOME/.dbt"
```

### Generate and merge LLM tests
```bash
python -m llm_tests.generate_dbt_tests
```

### Run comparator experiment
```bash
python -m experiments.run_multibatch_anomaly_experiment
```

### Run usefulness audit
```bash
python -m experiments.run_generated_test_usefulness_audit
```

### Run C5 stability
```bash
python experiments/c5_stability/run_c5_stability.py
python experiments/c5_stability/summarize_c5_results.py
```

## Key artifacts

- `dbt_project/generated_tests/generation_summary.json`
- `dbt_project/schema_versions/schema.manual_baseline.yml`
- `dbt_project/schema_versions/schema.manual_expanded.yml`
- `dbt_project/schema_versions/schema.llm_merged.yml`
- `experiments/multibatch_anomaly_results/final_summary.json`
- `experiments/usefulness_audit/generated_test_usefulness_summary.json`
- `experiments/runtime_logs/latest_pipeline_runtime.json`
- `experiments/c5_stability/c5_aggregate_summary.json`

## Recommended GitHub contents

Keep in the repo:
- source code
- schema versions
- prompts
- setup docs
- small summary artifacts needed for reproducibility

Do **not** commit:
- `.env`
- real credentials
- `.venv/`
- large local databases
- Airflow runtime junk
- `__pycache__/`

## Troubleshooting

### `Could not find profile named ...`
Check:
- `dbt_project/dbt_project.yml`
- your local `profiles.yml`
- `--profiles-dir`

### `Env var required but not provided`
Make sure `.env` is loaded into the current shell session.

### DuckDB file lock on Windows
Use a copied DuckDB file for generation or experiments instead of the live `dbt_project/target/dbt.duckdb`.

### `No module named llm_tests...`
Run modules with `-m`, for example:
```bash
python -m llm_tests.generate_dbt_tests
```
