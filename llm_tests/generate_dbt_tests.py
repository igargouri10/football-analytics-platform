import json
import os
from pathlib import Path

import duckdb
import yaml
from openai import OpenAI

from llm_tests.prompts import SYSTEM_PROMPT, build_user_prompt

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from experiments.runtime_logger import append_stage_result, utc_now_iso


DUCKDB_PATH = Path("dbt_project/target/dbt.duckdb")
OUTPUT_DIR = Path("dbt_project/generated_tests")
TARGET_MODELS = [
    "dim_teams",
    "fct_matches",
    "fct_training_dataset",
]


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(f"DuckDB file not found: {DUCKDB_PATH}")
    return duckdb.connect(str(DUCKDB_PATH))


def get_columns(con: duckdb.DuckDBPyConnection, model_name: str) -> list[dict]:
    rows = con.execute(f"DESCRIBE main.{model_name}").fetchall()
    cols = []
    for row in rows:
        cols.append({
            "name": row[0],
            "type": row[1],
        })
    return cols


def get_sample_rows(con: duckdb.DuckDBPyConnection, model_name: str, limit: int = 3) -> list[dict]:
    result = con.execute(f"SELECT * FROM main.{model_name} LIMIT {limit}")
    rows = result.fetchall()
    col_names = [d[0] for d in result.description]

    samples = []
    for row in rows:
        formatted = {}
        for key, value in zip(col_names, row):
            if hasattr(value, "isoformat"):
                formatted[key] = value.isoformat()
            else:
                formatted[key] = value
        samples.append(formatted)
    return samples


def call_llm(model_name: str, columns: list[dict], sample_rows: list[dict]) -> str:
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    user_prompt = build_user_prompt(model_name, columns, sample_rows)

    response = client.responses.create(
        model="gpt-4.1-mini",
        input=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.2,
    )

    return response.output_text.strip()


def validate_yaml(yaml_text: str, expected_model: str) -> dict:
    parsed = yaml.safe_load(yaml_text)

    if not isinstance(parsed, dict):
        raise ValueError("Generated YAML is not a dictionary")

    if "version" not in parsed or "models" not in parsed:
        raise ValueError("Generated YAML missing required top-level keys")

    models = parsed.get("models")
    if not isinstance(models, list) or len(models) != 1:
        raise ValueError("Generated YAML must contain exactly one model entry")

    model_entry = models[0]
    if model_entry.get("name") != expected_model:
        raise ValueError(
            f"Expected model name {expected_model}, got {model_entry.get('name')}"
        )

    return parsed


def save_yaml(model_name: str, yaml_text: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"{model_name}_llm_tests.yml"
    output_path.write_text(yaml_text, encoding="utf-8")
    return output_path


def generate_for_model(con: duckdb.DuckDBPyConnection, model_name: str) -> dict:
    columns = get_columns(con, model_name)
    sample_rows = get_sample_rows(con, model_name)

    yaml_text = call_llm(model_name, columns, sample_rows)
    validate_yaml(yaml_text, model_name)
    output_path = save_yaml(model_name, yaml_text)

    return {
        "model": model_name,
        "status": "success",
        "output_file": str(output_path),
        "columns": columns,
        "sample_rows": sample_rows,
    }


def generate_all_tests() -> list[dict]:
    start_time = utc_now_iso()
    con = get_duckdb_connection()
    results = []

    try:
        for model_name in TARGET_MODELS:
            try:
                result = generate_for_model(con, model_name)
                results.append(result)
                print(f"[OK] Generated tests for {model_name}: {result['output_file']}")
            except Exception as e:
                results.append({
                    "model": model_name,
                    "status": "failed",
                    "error": str(e),
                })
                print(f"[FAIL] {model_name}: {e}")
    finally:
        con.close()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = OUTPUT_DIR / "generation_summary.json"
    summary_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\nSaved summary to {summary_path}")

    success_count = sum(1 for r in results if r.get("status") == "success")
    failed_count = sum(1 for r in results if r.get("status") == "failed")
    status = "success" if failed_count == 0 else "partial_success"

    append_stage_result(
        stage_name="generate_llm_tests",
        start_time=start_time,
        end_time=utc_now_iso(),
        status=status,
        metadata={
            "total_models": len(results),
            "success_count": success_count,
            "failed_count": failed_count,
            "summary_file": str(summary_path),
        },
    )

    return results


if __name__ == "__main__":
    generate_all_tests()