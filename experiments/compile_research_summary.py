import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from experiments.runtime_logger import append_stage_result, utc_now_iso

OUT_DIR = ROOT / "experiments" / "research_artifacts"
OUT_DIR.mkdir(parents=True, exist_ok=True)

RUNTIME_PATH = ROOT / "experiments" / "runtime_logs" / "latest_pipeline_runtime.json"
GEN_PATH = ROOT / "dbt_project" / "generated_tests" / "generation_summary.json"
MIGRATION_PATH = ROOT / "experiments" / "duckdb_to_snowflake_migration_summary.json"
VALIDATION_PATH = ROOT / "experiments" / "duckdb_vs_migrated_snowflake_summary.json"
MULTIBATCH_PATH = ROOT / "experiments" / "multibatch_anomaly_results" / "final_summary.json"
AUDIT_PATH = ROOT / "experiments" / "usefulness_audit" / "generated_test_usefulness_summary.json"

FINAL_SUMMARY_PATH = OUT_DIR / "final_research_summary.json"


def load_json(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def latest_successful_stages(runtime_payload: dict) -> dict:
    """
    Keep only the latest successful record for each stage_name.
    Later entries overwrite earlier ones.
    """
    latest = {}
    for stage in runtime_payload.get("stages", []):
        if stage.get("status") == "success":
            latest[stage["stage_name"]] = stage
    return latest


def main():
    start_time = utc_now_iso()

    generation = load_json(GEN_PATH)
    migration = load_json(MIGRATION_PATH)
    validation = load_json(VALIDATION_PATH)
    multibatch = load_json(MULTIBATCH_PATH)
    audit = load_json(AUDIT_PATH)

    # First append this stage so the final runtime summary can include it
    compile_end_time = utc_now_iso()
    append_stage_result(
        stage_name="compile_research_summary",
        start_time=start_time,
        end_time=compile_end_time,
        status="success",
        metadata={
            "summary_file": str(FINAL_SUMMARY_PATH),
        },
    )

    # Now reload runtime and summarize only latest successful stage entries
    runtime = load_json(RUNTIME_PATH)
    successful_stage_map = latest_successful_stages(runtime)

    summary = {
        "artifact_paths": {
            "runtime": str(RUNTIME_PATH),
            "generation_summary": str(GEN_PATH),
            "migration_summary": str(MIGRATION_PATH),
            "validation_summary": str(VALIDATION_PATH),
            "multibatch_summary": str(MULTIBATCH_PATH),
            "usefulness_audit_summary": str(AUDIT_PATH),
        },
        "clean_pipeline_summary": {},
        "anomaly_detection_summary": {},
        "generated_test_usefulness_summary": {},
    }

    if generation is not None:
        summary["clean_pipeline_summary"]["llm_generation"] = {
            "models_attempted": len(generation),
            "models_successful": sum(1 for x in generation if x.get("status") == "success"),
            "models_failed": sum(1 for x in generation if x.get("status") != "success"),
        }

    if migration is not None:
        summary["clean_pipeline_summary"]["migration"] = {
            "tables_attempted": len(migration),
            "tables_loaded_successfully": sum(1 for x in migration if x.get("write_success")),
            "row_counts": {
                x["logical_name"]: {
                    "duckdb_rows": x["duckdb_rows"],
                    "snowflake_rows": x["snowflake_rows"],
                }
                for x in migration
            },
        }

    if validation is not None:
        summary["clean_pipeline_summary"]["validation"] = {
            "tables_compared": len(validation),
            "match_count": sum(1 for x in validation if x.get("status") == "MATCH"),
            "mismatch_count": sum(1 for x in validation if x.get("status") != "MATCH"),
        }

    summary["clean_pipeline_summary"]["runtime"] = {
        "instrumented_stage_count": len(successful_stage_map),
        "total_instrumented_duration_seconds": round(
            sum(stage.get("duration_seconds", 0) for stage in successful_stage_map.values()),
            3,
        ),
        "stage_durations": {
            stage_name: stage["duration_seconds"]
            for stage_name, stage in successful_stage_map.items()
        },
    }

    if multibatch is not None:
        summary["anomaly_detection_summary"] = {
            "manual_total_detected": multibatch["manual_only"]["aggregate"]["total_detected"],
            "manual_total_anomalies": multibatch["manual_only"]["aggregate"]["total_anomalies"],
            "manual_overall_detection_rate": multibatch["manual_only"]["aggregate"]["overall_detection_rate"],
            "llm_total_detected": multibatch["manual_plus_llm"]["aggregate"]["total_detected"],
            "llm_total_anomalies": multibatch["manual_plus_llm"]["aggregate"]["total_anomalies"],
            "llm_overall_detection_rate": multibatch["manual_plus_llm"]["aggregate"]["overall_detection_rate"],
            "absolute_additional_detections": multibatch["improvement_summary"]["absolute_additional_detections"],
            "absolute_detection_rate_gain": multibatch["improvement_summary"]["absolute_detection_rate_gain"],
            "relative_detection_count_improvement": multibatch["improvement_summary"]["relative_detection_count_improvement"],
            "batch_count": multibatch["manual_only"]["aggregate"]["batch_count"],
        }

    if audit is not None:
        summary["generated_test_usefulness_summary"] = audit["test_audit_summary"]

    FINAL_SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Saved final research summary to {FINAL_SUMMARY_PATH}")


if __name__ == "__main__":
    main()