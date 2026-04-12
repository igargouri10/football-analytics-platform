from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = ROOT / "dbt_project"
GENERATED_TESTS_DIR = DBT_DIR / "generated_tests"
SCHEMA_VERSIONS_DIR = DBT_DIR / "schema_versions"
MARTS_DIR = DBT_DIR / "models" / "marts"
EXPERIMENTS_DIR = ROOT / "experiments"

MANUAL_BASELINE_CANDIDATES = [
    SCHEMA_VERSIONS_DIR / "schema.manual_baseline.yml",
    MARTS_DIR / "schema.manual_backup.yml.txt",
]

MERGED_LLM_CANDIDATES = [
    SCHEMA_VERSIONS_DIR / "schema.llm_merged.yml",
    MARTS_DIR / "schema.llm_merged.yml.txt",
]

COMPARATOR_SUMMARY = (
    EXPERIMENTS_DIR / "multibatch_anomaly_results" / "final_summary.json"
)

OUTPUT_DIR = EXPERIMENTS_DIR / "usefulness_audit"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SUMMARY_PATH = OUTPUT_DIR / "generated_test_usefulness_summary.json"
DETAIL_PATH = OUTPUT_DIR / "generated_test_usefulness_detail.json"


def load_yaml_file(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    data = yaml.safe_load(text)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML root must be a mapping in {path}")
    return data


def resolve_existing(candidates: list[Path], label: str) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Could not find {label}. Checked:\n" + "\n".join(str(p) for p in candidates)
    )


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


def flatten_schema_tests(
    schema_data: dict[str, Any],
    source_path: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    models = schema_data.get("models", [])
    if not isinstance(models, list):
        return rows

    for model in models:
        if not isinstance(model, dict):
            continue

        model_name = model.get("name")
        if not model_name:
            continue

        columns = model.get("columns", [])
        if not isinstance(columns, list):
            continue

        for column in columns:
            if not isinstance(column, dict):
                continue

            column_name = column.get("name")
            if not column_name:
                continue

            tests = column.get("tests", [])
            if not isinstance(tests, list):
                continue

            for idx, test_entry in enumerate(tests):
                test_name, args = parse_test_entry(test_entry)
                rows.append(
                    {
                        "source_file": source_path,
                        "model": model_name,
                        "column": column_name,
                        "test_name": test_name,
                        "args": args,
                        "raw_test": canonicalize(test_entry),
                        "item_index": idx,
                    }
                )

    return rows


def normalized_key(item: dict[str, Any]) -> str:
    payload = {
        "model": item["model"],
        "column": item["column"],
        "test_name": item["test_name"],
        "args": canonicalize(item["args"]),
    }
    return json.dumps(payload, sort_keys=True)


def generic_detector_name(item: dict[str, Any]) -> str:
    # Matches the detector families used in the anomaly comparator.
    return f"{item['test_name']}_{item['model']}_{item['column']}"


def load_generated_items() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    valid_items: list[dict[str, Any]] = []
    invalid_items: list[dict[str, Any]] = []

    if not GENERATED_TESTS_DIR.exists():
        return valid_items, invalid_items

    for path in sorted(GENERATED_TESTS_DIR.iterdir()):
        if not path.is_file():
            continue
        if path.suffix.lower() not in {".yml", ".yaml"}:
            continue
        if not path.name.endswith(("_llm_tests.yml", "_llm_tests.yaml")):
            continue

        try:
            data = load_yaml_file(path)
            items = flatten_schema_tests(data, path.name)
            valid_items.extend(items)
        except Exception as exc:
            invalid_items.append(
                {
                    "source_file": path.name,
                    "error": str(exc),
                }
            )

    return valid_items, invalid_items


def load_baseline_keys() -> set[str]:
    baseline_path = resolve_existing(MANUAL_BASELINE_CANDIDATES, "manual baseline schema")
    baseline_data = load_yaml_file(baseline_path)
    baseline_items = flatten_schema_tests(baseline_data, baseline_path.name)
    return {normalized_key(item) for item in baseline_items}


def load_merged_keys() -> set[str]:
    merged_path = resolve_existing(MERGED_LLM_CANDIDATES, "merged LLM schema")
    merged_data = load_yaml_file(merged_path)
    merged_items = flatten_schema_tests(merged_data, merged_path.name)
    return {normalized_key(item) for item in merged_items}


def load_useful_detector_families() -> set[str]:
    if not COMPARATOR_SUMMARY.exists():
        return set()

    summary = json.loads(COMPARATOR_SUMMARY.read_text(encoding="utf-8"))

    manual_only_batches = {
        row["batch_id"]: row
        for row in summary["manual_only"]["per_batch_results"]
    }
    manual_plus_llm_batches = {
        row["batch_id"]: row
        for row in summary["manual_plus_llm"]["per_batch_results"]
    }

    useful_families: set[str] = set()

    for batch_id, llm_batch in manual_plus_llm_batches.items():
        manual_batch = manual_only_batches.get(batch_id)
        if manual_batch is None:
            continue

        manual_results = {
            row["anomaly_id"]: row for row in manual_batch.get("detection_results", [])
        }

        for llm_row in llm_batch.get("detection_results", []):
            anomaly_id = llm_row["anomaly_id"]
            llm_detected = bool(llm_row.get("detected", False))
            manual_detected = bool(manual_results.get(anomaly_id, {}).get("detected", False))

            if llm_detected and not manual_detected:
                for detector in llm_row.get("detecting_tests", []):
                    useful_families.add(detector)

    return useful_families


def classify_items() -> dict[str, Any]:
    generated_items, invalid_files = load_generated_items()
    baseline_keys = load_baseline_keys()
    merged_keys = load_merged_keys()
    useful_families = load_useful_detector_families()

    detail_rows: list[dict[str, Any]] = []

    useful_count = 0
    redundant_count = 0
    low_value_count = 0
    invalid_count = 0

    for bad in invalid_files:
        invalid_count += 1
        detail_rows.append(
            {
                "source_file": bad["source_file"],
                "model": None,
                "column": None,
                "test_name": None,
                "generic_detector": None,
                "classification": "invalid",
                "reason": bad["error"],
            }
        )

    for item in generated_items:
        key = normalized_key(item)
        detector = generic_detector_name(item)

        if key not in merged_keys:
            classification = "invalid"
            reason = "Generated item was not found in the merged executable schema."
            invalid_count += 1
        elif key in baseline_keys:
            classification = "redundant"
            reason = "Generated item duplicates a manual baseline test."
            redundant_count += 1
        elif detector in useful_families:
            classification = "useful"
            reason = "Generated item maps to a detector family that added coverage beyond the weak baseline."
            useful_count += 1
        else:
            classification = "low_value"
            reason = "Generated item is executable but did not demonstrate incremental detection value in the implemented anomaly batches."
            low_value_count += 1

        detail_rows.append(
            {
                "source_file": item["source_file"],
                "model": item["model"],
                "column": item["column"],
                "test_name": item["test_name"],
                "generic_detector": detector,
                "classification": classification,
                "reason": reason,
                "args": item["args"],
            }
        )

    summary = {
        "total_generated_items": len(generated_items) + len(invalid_files),
        "useful_count": useful_count,
        "redundant_count": redundant_count,
        "low_value_count": low_value_count,
        "invalid_count": invalid_count,
        "useful_detector_families": sorted(useful_families),
        "generated_source_files": sorted(
            {row["source_file"] for row in detail_rows if row["source_file"]}
        ),
        "classification_counts": {
            "useful": useful_count,
            "redundant": redundant_count,
            "low_value": low_value_count,
            "invalid": invalid_count,
        },
    }

    return {
        "summary": summary,
        "detail": detail_rows,
    }


def main() -> None:
    payload = classify_items()

    SUMMARY_PATH.write_text(
        json.dumps(payload["summary"], indent=2),
        encoding="utf-8",
    )
    DETAIL_PATH.write_text(
        json.dumps(payload["detail"], indent=2),
        encoding="utf-8",
    )

    print(f"Wrote summary to {SUMMARY_PATH}")
    print(f"Wrote detail to {DETAIL_PATH}")


if __name__ == "__main__":
    main()