import json
import shutil
import subprocess
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = ROOT / "dbt_project"
TARGET_DB = DBT_DIR / "target" / "dbt.duckdb"
CLEAN_DB = DBT_DIR / "target" / "dbt.clean.duckdb"

ACTIVE_SCHEMA = DBT_DIR / "models" / "marts" / "schema.yml"
MANUAL_SCHEMA = DBT_DIR / "models" / "marts" / "schema.manual_backup.yml.txt"
LLM_SCHEMA = DBT_DIR / "models" / "marts" / "schema.llm_merged.yml.txt"

OUTPUT_DIR = ROOT / "experiments" / "multibatch_anomaly_results"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Use prefixes for long dbt-generated names such as accepted_values_...
ANOMALY_DETECTORS = {
    "null_dim_teams_team_id": ["not_null_dim_teams_team_id"],
    "duplicate_dim_teams_team_id": ["unique_dim_teams_team_id"],
    "null_dim_teams_team_name": ["not_null_dim_teams_team_name"],
    "null_fct_matches_match_id": ["not_null_fct_matches_match_id", "not_null_fct_training_dataset_match_id"],
    "duplicate_fct_matches_match_id": ["unique_fct_matches_match_id", "unique_fct_training_dataset_match_id"],
    "null_fct_matches_match_status": ["not_null_fct_matches_match_status"],
    "invalid_fct_matches_match_status": ["accepted_values_fct_matches_match_status"],
    "null_fct_matches_home_team_id": [
        "not_null_fct_matches_home_team_id",
        "not_null_fct_training_dataset_home_team_id",
    ],
    "null_fct_matches_away_team_id": [
        "not_null_fct_matches_away_team_id",
        "not_null_fct_training_dataset_away_team_id",
    ],
    "null_fct_matches_match_date": [
        "not_null_fct_matches_match_date",
        "not_null_fct_training_dataset_match_date",
    ],
}

# Three batches:
# 1) baseline-friendly key anomalies
# 2) semantic/domain/completeness anomalies mostly covered by LLM-generated tests
# 3) mixed anomalies across dimension/fact/downstream propagation
BATCHES = [
    {
        "batch_id": "batch_A_key_integrity",
        "description": "Core key-integrity anomalies in dimension and fact tables.",
    },
    {
        "batch_id": "batch_B_semantic_domain",
        "description": "Semantic/domain and completeness anomalies primarily targeted by the LLM-expanded test layer.",
    },
    {
        "batch_id": "batch_C_mixed_full_stack",
        "description": "Mixed anomalies spanning dimension, fact, and downstream propagated effects.",
    },
]


def ensure_prereqs():
    missing = []
    for path in [CLEAN_DB, MANUAL_SCHEMA, LLM_SCHEMA]:
        if not path.exists():
            missing.append(str(path))
    if missing:
        raise FileNotFoundError(
            "Missing required experiment inputs:\n" + "\n".join(missing)
        )


def restore_clean_db():
    shutil.copyfile(CLEAN_DB, TARGET_DB)


def restore_schema(schema_source: Path):
    shutil.copyfile(schema_source, ACTIVE_SCHEMA)


def parse_failed_tests(stdout: str):
    failed = []

    for line in stdout.splitlines():
        if " FAIL " not in line:
            continue

        parts = line.split()
        if "FAIL" not in parts:
            continue

        idx = parts.index("FAIL")
        if idx + 1 >= len(parts):
            continue

        candidate = parts[idx + 1]

        # dbt can emit: FAIL 1 test_name ...
        if candidate.isdigit() and idx + 2 < len(parts):
            candidate = parts[idx + 2]

        if candidate not in failed:
            failed.append(candidate)

    return failed


def detector_triggered(detector_prefixes, failed_tests):
    for detector in detector_prefixes:
        for failed in failed_tests:
            if failed.startswith(detector) or detector in failed:
                return True
    return False


def score_detection(batch_manifest, failed_tests):
    scored = []
    for item in batch_manifest:
        anomaly_id = item["id"]
        detectors = ANOMALY_DETECTORS[anomaly_id]
        detected = detector_triggered(detectors, failed_tests)
        scored.append({
            "anomaly_id": anomaly_id,
            "detected": detected,
            "detecting_tests": detectors,
            "target": item["target"],
        })
    return scored


def run_dbt_test(condition_name: str, batch_id: str):
    cmd = ["dbt", "test", "--target", "dev", "--no-partial-parse"]
    proc = subprocess.run(
        cmd,
        cwd=str(DBT_DIR),
        capture_output=True,
        text=True,
    )

    log_path = OUTPUT_DIR / f"{batch_id}__{condition_name}_dbt_test.log"
    log_path.write_text(proc.stdout + "\n" + proc.stderr, encoding="utf-8")

    failed_tests = parse_failed_tests(proc.stdout)
    return {
        "condition": condition_name,
        "batch_id": batch_id,
        "returncode": proc.returncode,
        "failed_tests": failed_tests,
        "log_file": str(log_path),
    }


def inject_batch(db_path: Path, batch_id: str):
    con = duckdb.connect(str(db_path))
    manifest = []

    try:
        team_ids = [row[0] for row in con.execute("""
            SELECT team_id
            FROM main.dim_teams
            ORDER BY team_id
        """).fetchall()]

        team_pairs = con.execute("""
            SELECT team_id, team_name
            FROM main.dim_teams
            ORDER BY team_id
        """).fetchall()

        match_ids = [row[0] for row in con.execute("""
            SELECT match_id
            FROM main.fct_matches
            ORDER BY match_id
        """).fetchall()]

        if batch_id == "batch_A_key_integrity":
            # Uses early rows
            t1, t2 = team_pairs[0], team_pairs[1]
            m1, m2 = match_ids[0], match_ids[1]

            # 1) null_dim_teams_team_id
            con.execute("UPDATE main.dim_teams SET team_id = NULL WHERE team_id = ?", [t2[0]])
            manifest.append({"id": "null_dim_teams_team_id", "target": f"dim_teams.team_id={t2[0]}"})

            # 2) duplicate_dim_teams_team_id
            con.execute(
                "INSERT INTO main.dim_teams (team_id, team_name) VALUES (?, ?)",
                [t1[0], f"{t1[1]}_DUP"],
            )
            manifest.append({"id": "duplicate_dim_teams_team_id", "target": f"dim_teams.team_id={t1[0]}"})

            # 3) null_fct_matches_match_id
            con.execute("UPDATE main.fct_matches SET match_id = NULL WHERE match_id = ?", [m2])
            manifest.append({"id": "null_fct_matches_match_id", "target": f"fct_matches.match_id={m2}"})

            # 4) duplicate_fct_matches_match_id
            con.execute("""
                INSERT INTO main.fct_matches
                SELECT *
                FROM main.fct_matches
                WHERE match_id = ?
            """, [m1])
            manifest.append({"id": "duplicate_fct_matches_match_id", "target": f"fct_matches.match_id={m1}"})

        elif batch_id == "batch_B_semantic_domain":
            # Uses middle rows, focused on LLM-added semantic/completeness tests
            t3 = team_pairs[3]
            m3, m4, m5, m6, m7, m8 = match_ids[10:16]

            # 1) null_dim_teams_team_name
            con.execute("UPDATE main.dim_teams SET team_name = NULL WHERE team_id = ?", [t3[0]])
            manifest.append({"id": "null_dim_teams_team_name", "target": f"dim_teams.team_id={t3[0]}"})

            # 2) null_fct_matches_match_status
            con.execute("UPDATE main.fct_matches SET match_status = NULL WHERE match_id = ?", [m3])
            manifest.append({"id": "null_fct_matches_match_status", "target": f"fct_matches.match_id={m3}"})

            # 3) invalid_fct_matches_match_status
            con.execute("UPDATE main.fct_matches SET match_status = 'BROKEN_STATUS' WHERE match_id = ?", [m4])
            manifest.append({"id": "invalid_fct_matches_match_status", "target": f"fct_matches.match_id={m4}"})

            # 4) null_fct_matches_home_team_id
            con.execute("UPDATE main.fct_matches SET home_team_id = NULL WHERE match_id = ?", [m5])
            manifest.append({"id": "null_fct_matches_home_team_id", "target": f"fct_matches.match_id={m5}"})

            # 5) null_fct_matches_away_team_id
            con.execute("UPDATE main.fct_matches SET away_team_id = NULL WHERE match_id = ?", [m6])
            manifest.append({"id": "null_fct_matches_away_team_id", "target": f"fct_matches.match_id={m6}"})

            # 6) null_fct_matches_match_date
            con.execute("UPDATE main.fct_matches SET match_date = NULL WHERE match_id = ?", [m7])
            manifest.append({"id": "null_fct_matches_match_date", "target": f"fct_matches.match_id={m7}"})

        elif batch_id == "batch_C_mixed_full_stack":
            # Uses later rows, mixed anomalies
            t4, t5 = team_pairs[4], team_pairs[5]
            m9, m10, m11, m12 = match_ids[20:24]

            # 1) null_dim_teams_team_id
            con.execute("UPDATE main.dim_teams SET team_id = NULL WHERE team_id = ?", [t5[0]])
            manifest.append({"id": "null_dim_teams_team_id", "target": f"dim_teams.team_id={t5[0]}"})

            # 2) duplicate_dim_teams_team_id
            con.execute(
                "INSERT INTO main.dim_teams (team_id, team_name) VALUES (?, ?)",
                [t4[0], f"{t4[1]}_DUP2"],
            )
            manifest.append({"id": "duplicate_dim_teams_team_id", "target": f"dim_teams.team_id={t4[0]}"})

            # 3) null_dim_teams_team_name
            con.execute("UPDATE main.dim_teams SET team_name = NULL WHERE team_id = ?", [t4[0]])
            manifest.append({"id": "null_dim_teams_team_name", "target": f"dim_teams.team_id={t4[0]}"})

            # 4) duplicate_fct_matches_match_id
            con.execute("""
                INSERT INTO main.fct_matches
                SELECT *
                FROM main.fct_matches
                WHERE match_id = ?
            """, [m9])
            manifest.append({"id": "duplicate_fct_matches_match_id", "target": f"fct_matches.match_id={m9}"})

            # 5) null_fct_matches_home_team_id
            con.execute("UPDATE main.fct_matches SET home_team_id = NULL WHERE match_id = ?", [m10])
            manifest.append({"id": "null_fct_matches_home_team_id", "target": f"fct_matches.match_id={m10}"})

            # 6) null_fct_matches_match_date
            con.execute("UPDATE main.fct_matches SET match_date = NULL WHERE match_id = ?", [m11])
            manifest.append({"id": "null_fct_matches_match_date", "target": f"fct_matches.match_id={m11}"})

        else:
            raise ValueError(f"Unknown batch_id: {batch_id}")

    finally:
        con.close()

    return manifest


def run_condition_for_batch(batch_id: str, condition_name: str, schema_source: Path):
    restore_clean_db()
    restore_schema(schema_source)

    batch_manifest = inject_batch(TARGET_DB, batch_id)
    run_info = run_dbt_test(condition_name, batch_id)
    detection = score_detection(batch_manifest, run_info["failed_tests"])

    detected_count = sum(1 for d in detection if d["detected"])
    total_anomalies = len(detection)

    result = {
        "batch_id": batch_id,
        "condition": condition_name,
        "schema_source": str(schema_source),
        "returncode": run_info["returncode"],
        "failed_tests": run_info["failed_tests"],
        "log_file": run_info["log_file"],
        "anomaly_manifest": batch_manifest,
        "detection_results": detection,
        "detected_count": detected_count,
        "total_anomalies": total_anomalies,
        "detection_rate": round(detected_count / total_anomalies, 4) if total_anomalies else None,
    }

    out_path = OUTPUT_DIR / f"{batch_id}__{condition_name}_summary.json"
    out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"[{batch_id}][{condition_name}] detected {detected_count}/{total_anomalies}")
    print(f"[{batch_id}][{condition_name}] failed tests: {run_info['failed_tests']}")
    print(f"[{batch_id}][{condition_name}] summary: {out_path}")

    return result


def summarize_condition_results(results):
    total_detected = sum(r["detected_count"] for r in results)
    total_anomalies = sum(r["total_anomalies"] for r in results)
    avg_rate = round(sum(r["detection_rate"] for r in results) / len(results), 4) if results else None

    return {
        "total_detected": total_detected,
        "total_anomalies": total_anomalies,
        "overall_detection_rate": round(total_detected / total_anomalies, 4) if total_anomalies else None,
        "average_batch_detection_rate": avg_rate,
        "batch_count": len(results),
    }


def main():
    ensure_prereqs()

    manual_results = []
    llm_results = []

    try:
        for batch in BATCHES:
            batch_id = batch["batch_id"]

            manual_results.append(
                run_condition_for_batch(batch_id, "manual_only", MANUAL_SCHEMA)
            )

            llm_results.append(
                run_condition_for_batch(batch_id, "manual_plus_llm", LLM_SCHEMA)
            )

        manual_summary = summarize_condition_results(manual_results)
        llm_summary = summarize_condition_results(llm_results)

        improvement = {
            "absolute_additional_detections": llm_summary["total_detected"] - manual_summary["total_detected"],
            "absolute_detection_rate_gain": round(
                llm_summary["overall_detection_rate"] - manual_summary["overall_detection_rate"], 4
            ) if manual_summary["overall_detection_rate"] is not None else None,
            "relative_detection_count_improvement": round(
                ((llm_summary["total_detected"] - manual_summary["total_detected"]) / manual_summary["total_detected"]) * 100,
                2,
            ) if manual_summary["total_detected"] > 0 else None,
        }

        final_summary = {
            "batches": BATCHES,
            "manual_only": {
                "per_batch_results": manual_results,
                "aggregate": manual_summary,
            },
            "manual_plus_llm": {
                "per_batch_results": llm_results,
                "aggregate": llm_summary,
            },
            "improvement_summary": improvement,
        }

        final_path = OUTPUT_DIR / "final_summary.json"
        final_path.write_text(json.dumps(final_summary, indent=2), encoding="utf-8")
        print(f"\nSaved final summary to {final_path}")

    finally:
        restore_clean_db()
        restore_schema(LLM_SCHEMA if LLM_SCHEMA.exists() else ACTIVE_SCHEMA)


if __name__ == "__main__":
    main()