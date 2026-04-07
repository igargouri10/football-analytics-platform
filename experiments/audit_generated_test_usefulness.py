import csv
import json
from collections import Counter
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]

GEN_SUMMARY_PATH = ROOT / "dbt_project" / "generated_tests" / "generation_summary.json"
BASE_SCHEMA_PATH = ROOT / "dbt_project" / "models" / "marts" / "schema.manual_backup.yml.txt"
MULTIBATCH_SUMMARY_PATH = ROOT / "experiments" / "multibatch_anomaly_results" / "final_summary.json"

OUTPUT_DIR = ROOT / "experiments" / "usefulness_audit"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

JSON_OUT = OUTPUT_DIR / "generated_test_usefulness_summary.json"
CSV_OUT = OUTPUT_DIR / "generated_test_usefulness_details.csv"


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def load_yaml(path: Path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def normalize_args(args):
    return json.dumps(args or {}, sort_keys=True, ensure_ascii=False)


def parse_test_item(test_item):
    if isinstance(test_item, str):
        return test_item, {}
    if isinstance(test_item, dict):
        # one-key dbt generic test mapping
        test_name = next(iter(test_item.keys()))
        args = test_item[test_name] or {}
        return test_name, args
    raise ValueError(f"Unsupported test item: {test_item!r}")


def detector_prefix(model_name, column_name, test_name):
    return f"{test_name}_{model_name}_{column_name}"


def exact_signature(model_name, column_name, test_name, args):
    return f"{model_name}|{column_name}|{test_name}|{normalize_args(args)}"


def collect_schema_tests(schema_data):
    tests = []
    for model in schema_data.get("models", []) or []:
        model_name = model.get("name")
        for col in model.get("columns", []) or []:
            column_name = col.get("name")
            for test_item in col.get("tests", []) or []:
                test_name, args = parse_test_item(test_item)
                tests.append({
                    "model": model_name,
                    "column": column_name,
                    "test_name": test_name,
                    "args": args,
                    "detector_prefix": detector_prefix(model_name, column_name, test_name),
                    "exact_signature": exact_signature(model_name, column_name, test_name, args),
                })
    return tests


def collect_generated_tests(gen_summary):
    generated_tests = []
    invalid_outputs = []

    for item in gen_summary:
        model_name = item.get("model")
        status = item.get("status")
        rel_output_file = item.get("output_file")

        if status != "success":
            invalid_outputs.append({
                "model": model_name,
                "status": status,
                "reason": item.get("error", "generation_failed"),
                "output_file": rel_output_file,
            })
            continue

        output_path = ROOT / Path(rel_output_file)
        if not output_path.exists():
            invalid_outputs.append({
                "model": model_name,
                "status": "failed",
                "reason": "output_file_missing",
                "output_file": rel_output_file,
            })
            continue

        try:
            yaml_data = load_yaml(output_path)
        except Exception as e:
            invalid_outputs.append({
                "model": model_name,
                "status": "failed",
                "reason": f"yaml_parse_error: {e}",
                "output_file": rel_output_file,
            })
            continue

        for test in collect_schema_tests(yaml_data):
            test["source_file"] = rel_output_file
            generated_tests.append(test)

    return generated_tests, invalid_outputs


def build_incremental_detector_set(multibatch_summary):
    """
    Return detector prefixes that produced incremental value:
    detected by manual+LLM but not by manual-only for the same batch+anomaly.
    """
    manual_batches = {
        r["batch_id"]: r for r in multibatch_summary["manual_only"]["per_batch_results"]
    }
    llm_batches = {
        r["batch_id"]: r for r in multibatch_summary["manual_plus_llm"]["per_batch_results"]
    }

    incremental = set()
    llm_any_detected = set()

    for batch_id, llm_batch in llm_batches.items():
        manual_batch = manual_batches[batch_id]

        manual_by_anomaly = {
            r["anomaly_id"]: r for r in manual_batch["detection_results"]
        }
        llm_by_anomaly = {
            r["anomaly_id"]: r for r in llm_batch["detection_results"]
        }

        for anomaly_id, llm_result in llm_by_anomaly.items():
            manual_result = manual_by_anomaly[anomaly_id]

            if llm_result["detected"]:
                for prefix in llm_result["detecting_tests"]:
                    llm_any_detected.add(prefix)

            if llm_result["detected"] and not manual_result["detected"]:
                for prefix in llm_result["detecting_tests"]:
                    incremental.add(prefix)

    return incremental, llm_any_detected


def main():
    gen_summary = load_json(GEN_SUMMARY_PATH)
    base_schema = load_yaml(BASE_SCHEMA_PATH)
    multibatch_summary = load_json(MULTIBATCH_SUMMARY_PATH)

    baseline_tests = collect_schema_tests(base_schema)
    baseline_signatures = {t["exact_signature"] for t in baseline_tests}

    generated_tests, invalid_outputs = collect_generated_tests(gen_summary)

    incremental_detector_prefixes, llm_any_detected_prefixes = build_incremental_detector_set(
        multibatch_summary
    )

    audited = []

    for test in generated_tests:
        sig = test["exact_signature"]
        prefix = test["detector_prefix"]

        if sig in baseline_signatures:
            classification = "redundant"
            rationale = "Exact duplicate of a manual baseline test."
        elif prefix in incremental_detector_prefixes:
            classification = "useful"
            rationale = "Linked to anomaly types detected by manual+LLM but missed by manual-only."
        elif prefix in llm_any_detected_prefixes:
            classification = "executable_but_low_value"
            rationale = "Executed and fired in anomaly experiments, but did not add incremental detection beyond the baseline."
        else:
            classification = "executable_but_low_value"
            rationale = "Executable generated test, but no direct incremental-detection evidence in current anomaly batches."

        audited.append({
            "model": test["model"],
            "column": test["column"],
            "test_name": test["test_name"],
            "args": test["args"],
            "source_file": test["source_file"],
            "detector_prefix": prefix,
            "classification": classification,
            "rationale": rationale,
        })

    # Add invalid generation outputs, if any
    for invalid in invalid_outputs:
        audited.append({
            "model": invalid["model"],
            "column": None,
            "test_name": None,
            "args": None,
            "source_file": invalid["output_file"],
            "detector_prefix": None,
            "classification": "invalid_or_non_executable",
            "rationale": invalid["reason"],
        })

    counter = Counter(item["classification"] for item in audited)

    total_generated_items = len(audited)
    useful_count = counter.get("useful", 0)
    redundant_count = counter.get("redundant", 0)
    low_value_count = counter.get("executable_but_low_value", 0)
    invalid_count = counter.get("invalid_or_non_executable", 0)

    nonredundant_executable = useful_count + low_value_count

    summary = {
        "source_artifacts": {
            "generation_summary": str(GEN_SUMMARY_PATH),
            "baseline_schema": str(BASE_SCHEMA_PATH),
            "multibatch_summary": str(MULTIBATCH_SUMMARY_PATH),
        },
        "llm_generation_summary": {
            "models_attempted": len(gen_summary),
            "models_successful": sum(1 for x in gen_summary if x.get("status") == "success"),
            "models_failed": sum(1 for x in gen_summary if x.get("status") != "success"),
        },
        "multibatch_detection_context": {
            "manual_total_detected": multibatch_summary["manual_only"]["aggregate"]["total_detected"],
            "manual_total_anomalies": multibatch_summary["manual_only"]["aggregate"]["total_anomalies"],
            "manual_overall_detection_rate": multibatch_summary["manual_only"]["aggregate"]["overall_detection_rate"],
            "llm_total_detected": multibatch_summary["manual_plus_llm"]["aggregate"]["total_detected"],
            "llm_total_anomalies": multibatch_summary["manual_plus_llm"]["aggregate"]["total_anomalies"],
            "llm_overall_detection_rate": multibatch_summary["manual_plus_llm"]["aggregate"]["overall_detection_rate"],
            "relative_detection_count_improvement": multibatch_summary["improvement_summary"]["relative_detection_count_improvement"],
        },
        "test_audit_summary": {
            "total_generated_test_items": total_generated_items,
            "useful_count": useful_count,
            "redundant_count": redundant_count,
            "executable_but_low_value_count": low_value_count,
            "invalid_or_non_executable_count": invalid_count,
            "useful_percentage_of_all_generated": round(useful_count / total_generated_items, 4) if total_generated_items else None,
            "redundant_percentage_of_all_generated": round(redundant_count / total_generated_items, 4) if total_generated_items else None,
            "useful_percentage_of_nonredundant_executable": round(useful_count / nonredundant_executable, 4) if nonredundant_executable else None,
        },
        "incremental_detector_prefixes": sorted(incremental_detector_prefixes),
        "audited_tests": audited,
    }

    JSON_OUT.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    with CSV_OUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "model",
                "column",
                "test_name",
                "args",
                "source_file",
                "detector_prefix",
                "classification",
                "rationale",
            ],
        )
        writer.writeheader()
        for row in audited:
            writer.writerow(row)

    print(f"Saved JSON summary to {JSON_OUT}")
    print(f"Saved CSV details to {CSV_OUT}")
    print(json.dumps(summary["test_audit_summary"], indent=2))


if __name__ == "__main__":
    main()