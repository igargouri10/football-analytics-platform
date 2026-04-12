from __future__ import annotations

import json
import os
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import duckdb
import yaml
from openai import OpenAI

from llm_tests.prompts import SYSTEM_PROMPT, build_user_prompt
from experiments.runtime_logger import append_stage_result, utc_now_iso

DEFAULT_DUCKDB_PATH = ROOT / "dbt_project" / "target" / "dbt.duckdb"
OUTPUT_DIR = ROOT / "dbt_project" / "generated_tests"
SCHEMA_VERSIONS_DIR = ROOT / "dbt_project" / "schema_versions"
MARTS_DIR = ROOT / "dbt_project" / "models" / "marts"

ACTIVE_SCHEMA = MARTS_DIR / "schema.yml"
NORMALIZED_LLM_SCHEMA = SCHEMA_VERSIONS_DIR / "schema.llm_merged.yml"

MANUAL_BASELINE_CANDIDATES = [
    SCHEMA_VERSIONS_DIR / "schema.manual_baseline.yml",
    MARTS_DIR / "schema.manual_backup.yml.txt",
]

TARGET_MODELS = [
    "dim_teams",
    "fct_matches",
    "fct_training_dataset",
]


def get_duckdb_path() -> Path:
    env_path = os.environ.get("DBT_DUCKDB_PATH")
    if env_path:
        return Path(env_path)
    return DEFAULT_DUCKDB_PATH


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    duckdb_path = get_duckdb_path()
    if not duckdb_path.exists():
        raise FileNotFoundError(f"DuckDB file not found: {duckdb_path}")
    return duckdb.connect(str(duckdb_path), read_only=True)


def get_columns(con: duckdb.DuckDBPyConnection, model_name: str) -> list[dict[str, Any]]:
    rows = con.execute(f"DESCRIBE main.{model_name}").fetchall()
    return [{"name": row[0], "type": row[1]} for row in rows]


def get_sample_rows(
    con: duckdb.DuckDBPyConnection,
    model_name: str,
    limit: int = 3,
) -> list[dict[str, Any]]:
    result = con.execute(f"SELECT * FROM main.{model_name} LIMIT {limit}")
    rows = result.fetchall()
    col_names = [d[0] for d in result.description]

    samples: list[dict[str, Any]] = []
    for row in rows:
        formatted: dict[str, Any] = {}
        for key, value in zip(col_names, row):
            if hasattr(value, "isoformat"):
                formatted[key] = value.isoformat()
            else:
                formatted[key] = value
        samples.append(formatted)
    return samples


def call_llm(model_name: str, columns: list[dict[str, Any]], sample_rows: list[dict[str, Any]]) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set in the environment.")

    client = OpenAI(api_key=api_key)
    user_prompt = build_user_prompt(model_name, columns, sample_rows)

    response = client.responses.create(
        model="gpt-4.1-mini",
        input=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.2,
    )

    yaml_text = response.output_text.strip()
    if not yaml_text:
        raise ValueError(f"LLM returned empty output for model {model_name}.")
    return yaml_text


def validate_yaml(yaml_text: str, expected_model: str) -> dict[str, Any]:
    parsed = yaml.safe_load(yaml_text)

    if not isinstance(parsed, dict):
        raise ValueError("Generated YAML is not a dictionary.")

    if "version" not in parsed or "models" not in parsed:
        raise ValueError("Generated YAML missing required top-level keys.")

    models = parsed.get("models")
    if not isinstance(models, list) or len(models) != 1:
        raise ValueError("Generated YAML must contain exactly one model entry.")

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


def canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: canonicalize(value[k]) for k in sorted(value)}
    if isinstance(value, list):
        return [canonicalize(v) for v in value]
    return value


def parse_test_entry(test_entry: Any) -> tuple[str, dict[str, Any]]:
    if isinstance(test_entry, str):
        return test_entry, {}

    if isinstance(test_entry, dict) and len(test_entry) == 1:
        test_name = next(iter(test_entry))
        raw_args = test_entry[test_name]
        if raw_args is None:
            return test_name, {}
        if isinstance(raw_args, dict):
            return test_name, canonicalize(raw_args)
        return test_name, {"value": canonicalize(raw_args)}

    raise ValueError(f"Unsupported test entry format: {test_entry!r}")


def test_entry_key(test_entry: Any) -> str:
    test_name, args = parse_test_entry(test_entry)
    payload = {
        "test_name": test_name,
        "args": canonicalize(args),
    }
    return json.dumps(payload, sort_keys=True)


def count_tests_in_schema(schema_data: dict[str, Any]) -> int:
    count = 0
    for model in schema_data.get("models", []):
        if not isinstance(model, dict):
            continue
        for column in model.get("columns", []):
            if not isinstance(column, dict):
                continue
            tests = column.get("tests", [])
            if isinstance(tests, list):
                count += len(tests)
    return count


def resolve_manual_baseline_schema() -> Path:
    for candidate in MANUAL_BASELINE_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Could not find a manual baseline schema. Checked:\n"
        + "\n".join(str(p) for p in MANUAL_BASELINE_CANDIDATES)
    )


def load_yaml_file(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML root must be a mapping in {path}")
    return data


def get_or_create_model(schema_data: dict[str, Any], model_name: str) -> dict[str, Any]:
    models = schema_data.setdefault("models", [])
    if not isinstance(models, list):
        raise ValueError("Schema 'models' must be a list.")

    for model in models:
        if isinstance(model, dict) and model.get("name") == model_name:
            model.setdefault("columns", [])
            return model

    new_model = {"name": model_name, "columns": []}
    models.append(new_model)
    return new_model


def get_or_create_column(model_entry: dict[str, Any], column_name: str) -> dict[str, Any]:
    columns = model_entry.setdefault("columns", [])
    if not isinstance(columns, list):
        raise ValueError("Model 'columns' must be a list.")

    for column in columns:
        if isinstance(column, dict) and column.get("name") == column_name:
            column.setdefault("tests", [])
            return column

    new_column = {"name": column_name, "tests": []}
    columns.append(new_column)
    return new_column


def merge_generated_tests_into_schema() -> tuple[Path, int]:
    baseline_path = resolve_manual_baseline_schema()
    merged_schema = load_yaml_file(baseline_path)

    if "version" not in merged_schema:
        merged_schema["version"] = 2
    if "models" not in merged_schema:
        merged_schema["models"] = []

    generated_files = sorted(OUTPUT_DIR.glob("*_llm_tests.yml"))
    if not generated_files:
        raise FileNotFoundError(
            f"No generated YAML files were found under {OUTPUT_DIR}"
        )

    for generated_file in generated_files:
        generated_data = load_yaml_file(generated_file)

        for generated_model in generated_data.get("models", []):
            if not isinstance(generated_model, dict):
                continue

            model_name = generated_model.get("name")
            if not model_name:
                continue

            target_model = get_or_create_model(merged_schema, model_name)

            if generated_model.get("description") and not target_model.get("description"):
                target_model["description"] = generated_model["description"]

            for generated_column in generated_model.get("columns", []):
                if not isinstance(generated_column, dict):
                    continue

                column_name = generated_column.get("name")
                if not column_name:
                    continue

                target_column = get_or_create_column(target_model, column_name)

                if generated_column.get("description") and not target_column.get("description"):
                    target_column["description"] = generated_column["description"]

                existing_keys = {
                    test_entry_key(test_entry)
                    for test_entry in target_column.get("tests", [])
                }

                for test_entry in generated_column.get("tests", []):
                    key = test_entry_key(test_entry)
                    if key not in existing_keys:
                        target_column.setdefault("tests", []).append(deepcopy(test_entry))
                        existing_keys.add(key)

    SCHEMA_VERSIONS_DIR.mkdir(parents=True, exist_ok=True)
    MARTS_DIR.mkdir(parents=True, exist_ok=True)

    yaml_text = yaml.safe_dump(
        merged_schema,
        sort_keys=False,
        allow_unicode=True,
    )

    NORMALIZED_LLM_SCHEMA.write_text(yaml_text, encoding="utf-8")
    ACTIVE_SCHEMA.write_text(yaml_text, encoding="utf-8")

    merged_test_count = count_tests_in_schema(merged_schema)
    return NORMALIZED_LLM_SCHEMA, merged_test_count


def generate_for_model(con: duckdb.DuckDBPyConnection, model_name: str) -> dict[str, Any]:
    columns = get_columns(con, model_name)
    sample_rows = get_sample_rows(con, model_name)

    yaml_text = call_llm(model_name, columns, sample_rows)
    parsed = validate_yaml(yaml_text, model_name)
    output_path = save_yaml(model_name, yaml_text)

    return {
        "model": model_name,
        "status": "success",
        "output_file": str(output_path),
        "columns": columns,
        "sample_rows": sample_rows,
        "generated_test_count": count_tests_in_schema(parsed),
    }


def generate_all_tests() -> dict[str, Any]:
    start_time = utc_now_iso()
    con = get_duckdb_connection()
    results: list[dict[str, Any]] = []

    try:
        for model_name in TARGET_MODELS:
            try:
                result = generate_for_model(con, model_name)
                results.append(result)
                print(f"[OK] Generated tests for {model_name}: {result['output_file']}")
            except Exception as exc:
                results.append(
                    {
                        "model": model_name,
                        "status": "failed",
                        "error": str(exc),
                    }
                )
                print(f"[FAIL] {model_name}: {exc}")
    finally:
        con.close()

    success_count = sum(1 for r in results if r.get("status") == "success")
    failed_count = sum(1 for r in results if r.get("status") == "failed")
    total_generated_items = sum(
        int(r.get("generated_test_count", 0))
        for r in results
        if r.get("status") == "success"
    )

    merged_schema_file = None
    merged_schema_test_count = None

    if success_count > 0:
        merged_path, merged_count = merge_generated_tests_into_schema()
        merged_schema_file = str(merged_path)
        merged_schema_test_count = merged_count
        print(f"[OK] Merged generated tests into {merged_path}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = OUTPUT_DIR / "generation_summary.json"

    summary_payload = {
        "target_models": TARGET_MODELS,
        "duckdb_path_used": str(get_duckdb_path()),
        "total_models": len(results),
        "success_count": success_count,
        "failed_count": failed_count,
        "total_generated_items": total_generated_items,
        "merged_schema_file": merged_schema_file,
        "merged_schema_test_count": merged_schema_test_count,
        "results": results,
    }

    summary_path.write_text(
        json.dumps(summary_payload, indent=2),
        encoding="utf-8",
    )
    print(f"\nSaved summary to {summary_path}")

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
            "total_generated_items": total_generated_items,
            "summary_file": str(summary_path),
            "merged_schema_file": merged_schema_file,
            "merged_schema_test_count": merged_schema_test_count,
        },
    )

    return summary_payload


if __name__ == "__main__":
    generate_all_tests()