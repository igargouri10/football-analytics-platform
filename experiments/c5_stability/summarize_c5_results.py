from __future__ import annotations

import csv
import json
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
C5_DIR = ROOT / "experiments" / "c5_stability"
FROZEN_DIR = C5_DIR / "frozen_schema"
FRESH_DIR = C5_DIR / "fresh_generation"


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def flatten_leaves(data: Any, prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}

    if isinstance(data, dict):
        for k, v in data.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            out.update(flatten_leaves(v, key))
    elif isinstance(data, list):
        for i, v in enumerate(data):
            key = f"{prefix}[{i}]"
            out.update(flatten_leaves(v, key))
    else:
        out[prefix.lower()] = data

    return out


def find_first_value(data: dict[str, Any], aliases: list[str]) -> Any:
    flat = flatten_leaves(data)
    for alias in aliases:
        alias = alias.lower()
        for key, value in flat.items():
            if key.endswith(alias):
                return value
    return None


def safe_stats(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"mean": None, "std": None, "min": None, "max": None}
    if len(values) == 1:
        return {"mean": values[0], "std": 0.0, "min": values[0], "max": values[0]}
    return {
        "mean": round(mean(values), 4),
        "std": round(pstdev(values), 4),
        "min": min(values),
        "max": max(values),
    }


def extract_trial_record(trial_dir: Path) -> dict[str, Any]:
    manifest = read_json(trial_dir / "trial_manifest.json")
    comparator = read_json(trial_dir / "comparator_final_summary.json")

    usefulness_path = trial_dir / "generated_test_usefulness_summary.json"
    generation_path = trial_dir / "generation_summary.json"

    usefulness = read_json(usefulness_path) if usefulness_path.exists() else {}
    generation = read_json(generation_path) if generation_path.exists() else {}

    manual_only = comparator["manual_only"]["aggregate"]["total_detected"]
    manual_expanded = comparator["manual_expanded"]["aggregate"]["total_detected"]
    manual_plus_llm = comparator["manual_plus_llm"]["aggregate"]["total_detected"]

    record = {
        "name": trial_dir.name,
        "mode": manifest["mode"],
        "duration_seconds": manifest.get("duration_seconds"),
        "manual_only_detected": manual_only,
        "manual_expanded_detected": manual_expanded,
        "manual_plus_llm_detected": manual_plus_llm,
        "llm_matches_manual_expanded": manual_plus_llm == manual_expanded,
        "llm_beats_weak_baseline": manual_plus_llm > manual_only,
        "generation_total": find_first_value(
            generation,
            [
                "total_generated_tests",
                "total_generated_items",
                "generated_tests_total",
                "total_generated",
                "total_items",
            ],
        ),
        "useful_count": find_first_value(
            usefulness,
            ["useful_count", "useful_tests", "useful_items", "useful"],
        ),
        "redundant_count": find_first_value(
            usefulness,
            ["redundant_count", "redundant_tests", "redundant_items", "redundant"],
        ),
        "low_value_count": find_first_value(
            usefulness,
            [
                "low_value_count",
                "low_value_tests",
                "low_value_items",
                "executable_but_low_value",
                "low_value",
            ],
        ),
        "invalid_count": find_first_value(
            usefulness,
            [
                "invalid_count",
                "invalid_tests",
                "invalid_items",
                "invalid_or_non_executable",
                "invalid",
            ],
        ),
    }
    return record


def collect_records(base_dir: Path) -> list[dict[str, Any]]:
    records = []
    if not base_dir.exists():
        return records

    for item in sorted(base_dir.iterdir()):
        if not item.is_dir():
            continue
        if (item / "trial_manifest.json").exists() and (item / "comparator_final_summary.json").exists():
            records.append(extract_trial_record(item))
    return records


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def make_aggregate_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"trial_count": 0}

    llm_detected = [r["manual_plus_llm_detected"] for r in rows if r["manual_plus_llm_detected"] is not None]
    manual_only = [r["manual_only_detected"] for r in rows if r["manual_only_detected"] is not None]
    manual_expanded = [r["manual_expanded_detected"] for r in rows if r["manual_expanded_detected"] is not None]
    durations = [r["duration_seconds"] for r in rows if r["duration_seconds"] is not None]

    gen_total = [r["generation_total"] for r in rows if isinstance(r["generation_total"], (int, float))]
    useful = [r["useful_count"] for r in rows if isinstance(r["useful_count"], (int, float))]
    redundant = [r["redundant_count"] for r in rows if isinstance(r["redundant_count"], (int, float))]
    low_value = [r["low_value_count"] for r in rows if isinstance(r["low_value_count"], (int, float))]
    invalid = [r["invalid_count"] for r in rows if isinstance(r["invalid_count"], (int, float))]

    return {
        "trial_count": len(rows),
        "llm_matches_manual_expanded_count": sum(1 for r in rows if r["llm_matches_manual_expanded"]),
        "llm_beats_weak_baseline_count": sum(1 for r in rows if r["llm_beats_weak_baseline"]),
        "manual_only_detected_stats": safe_stats(manual_only),
        "manual_expanded_detected_stats": safe_stats(manual_expanded),
        "manual_plus_llm_detected_stats": safe_stats(llm_detected),
        "duration_seconds_stats": safe_stats(durations),
        "generation_total_stats": safe_stats(gen_total),
        "useful_count_stats": safe_stats(useful),
        "redundant_count_stats": safe_stats(redundant),
        "low_value_count_stats": safe_stats(low_value),
        "invalid_count_stats": safe_stats(invalid),
    }


def main() -> None:
    frozen_rows = collect_records(FROZEN_DIR)
    fresh_rows = collect_records(FRESH_DIR)

    write_csv(C5_DIR / "c5_frozen_schema_trials.csv", frozen_rows)
    write_csv(C5_DIR / "c5_fresh_generation_trials.csv", fresh_rows)

    aggregate = {
        "frozen_schema": make_aggregate_summary(frozen_rows),
        "fresh_generation": make_aggregate_summary(fresh_rows),
    }

    (C5_DIR / "c5_aggregate_summary.json").write_text(
        json.dumps(aggregate, indent=2),
        encoding="utf-8",
    )

    print("Wrote:")
    print(C5_DIR / "c5_frozen_schema_trials.csv")
    print(C5_DIR / "c5_fresh_generation_trials.csv")
    print(C5_DIR / "c5_aggregate_summary.json")


if __name__ == "__main__":
    main()