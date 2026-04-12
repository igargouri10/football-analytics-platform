# Setup Guide

## Python environment
```bash
python -m venv .venv
```

**Windows**
```powershell
.\.venv\Scripts\Activate.ps1
```

**macOS / Linux**
```bash
source .venv/bin/activate
```

Install requirements:
```bash
pip install -r requirements.txt
```

## Environment variables
Copy `.env.example` to `.env` and fill in:
- OpenAI credentials
- AWS credentials
- S3 bucket name
- Snowflake credentials

## dbt profiles
Copy `.dbt/profiles.example.yml` into your local dbt profile directory.

## Verify dbt
```bash
dbt debug --project-dir dbt_project --profiles-dir "$HOME/.dbt"
```

## Run local models
```bash
dbt run --project-dir dbt_project --profiles-dir "$HOME/.dbt"
dbt test --project-dir dbt_project --profiles-dir "$HOME/.dbt"
```

## Generate semantic tests
```bash
python -m llm_tests.generate_dbt_tests
```

## Run experiments
```bash
python -m experiments.run_multibatch_anomaly_experiment
python -m experiments.run_generated_test_usefulness_audit
```

## Run C5
```bash
python experiments/c5_stability/run_c5_stability.py
python experiments/c5_stability/summarize_c5_results.py
```
