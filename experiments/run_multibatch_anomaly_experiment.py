from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = ROOT / "dbt_project"
SCHEMA_VERSIONS_DIR = DBT_DIR / "schema_versions"
PROFILES_DIR = Path.home() / ".dbt"
ENV_FILE = ROOT / ".env"

OUTPUT_DIR = ROOT / "experiments" / "multibatch_anomaly_results"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TEMP_DB_DIR = OUTPUT_DIR / "temp_dbs"
TEMP_DB_DIR.mkdir(parents=True, exist_ok=True)

TARGET_DB = DBT_DIR / "target" / "dbt.duckdb"
CLEAN_DB = DBT_DIR / "target" / "dbt.clean.duckdb"

ACTIVE_SCHEMA = DBT_DIR / "models" / "marts" / "schema.yml"
MANUAL_BASELINE_SCHEMA = SCHEMA_VERSIONS_DIR / "schema.manual_baseline.yml"
MANUAL_EXPANDED_SCHEMA = SCHEMA_VERSIONS_DIR / "schema.manual_expanded.yml"

LLM_SCHEMA_CANDIDATES = [
    SCHEMA_VERSIONS_DIR / "schema.llm_merged.yml",
    DBT_DIR / "models" / "marts" / "schema.llm_merged.yml.txt",
]

RUN_RESULTS_PATH = DBT_DIR / "target" / "run_results.json"


# Use prefixes for long dbt-generated names such as accepted_values_...
ANOMALY_DETECTORS = {
    "null_dim_teams_team_id": ["not_null_dim_teams_team_id"],
    "duplicate_dim_teams_team_id": ["unique_dim_teams_team_id"],
    "null_dim_teams_team_name": ["not_null_dim_teams_team_name"],
    "null_fct_matches_match_id": [
        "not_null_fct_matches_match_id",
        "not_null_fct_training_dataset_match_id",
    ],
    "duplicate_fct_matches_match_id": [
        "unique_fct_matches_match_id",
        "unique_fct_training_dataset_match_id",
    ],
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

BATCHES = [
    {
        "batch_id": "batch_A_key_integrity",
        "description": "Core key-integrity anomalies in dimension and fact tables.",
    },
    {
        "batch_id": "batch_B_semantic_domain",
        "description": "Semantic/domain and completeness anomalies primarily targeted by the expanded test layer.",
    },
    {
        "batch_id": "batch_C_mixed_full_stack",
        "description": "Mixed anomalies spanning dimension, fact, and downstream propagated effects.",
    },
]


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        name, value = line.split("=", 1)
        name = name.strip()
        value = value.strip().strip('"').strip("'")

        if name and name not in os.environ:
            os.environ[name] = value

def resolve_llm_schema() -> Path:
    for candidate in LLM_SCHEMA_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Could not find an LLM merged schema file. Checked:\n"
        + "\n".join(str(p) for p in LLM_SCHEMA_CANDIDATES)
    )


def ensure_prereqs() -> dict[str, Path]:
    llm_schema = resolve_llm_schema()

    required = {
        "clean_db": CLEAN_DB,
        "manual_baseline": MANUAL_BASELINE_SCHEMA,
        "manual_expanded": MANUAL_EXPANDED_SCHEMA,
        "llm_schema": llm_schema,
        "profiles_dir": PROFILES_DIR / "profiles.yml",
    }

    missing = [str(path) for path in required.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing required experiment inputs:\n" + "\n".join(missing)
        )

    return {
        "manual_only": MANUAL_BASELINE_SCHEMA,
        "manual_expanded": MANUAL_EXPANDED_SCHEMA,
        "manual_plus_llm": llm_schema,
    }


def restore_clean_db(dest_db: Path) -> None:
    dest_db.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(CLEAN_DB, dest_db)

def get_temp_db_path(condition_name: str, batch_id: str) -> Path:
    return TEMP_DB_DIR / f"{batch_id}__{condition_name}.duckdb"


def restore_schema(schema_source: Path) -> None:
    shutil.copyfile(schema_source, ACTIVE_SCHEMA)


def parse_failed_tests_from_run_results() -> list[str]:
    if not RUN_RESULTS_PATH.exists():
        return []

    payload = json.loads(RUN_RESULTS_PATH.read_text(encoding="utf-8"))
    failed = []

    for result in payload.get("results", []):
        status = result.get("status", "").lower()
        if status not in {"fail", "error"}:
            continue

        unique_id = result.get("unique_id", "")
        if unique_id:
            failed.append(unique_id)

    return failed


def detector_triggered(detector_prefixes: list[str], failed_tests: list[str]) -> bool:
    for detector in detector_prefixes:
        for failed in failed_tests:
            if failed.startswith(detector) or detector in failed:
                return True
    return False


def score_detection(batch_manifest: list[dict], failed_tests: list[str]) -> list[dict]:
    scored = []
    for item in batch_manifest:
        anomaly_id = item["id"]
        detectors = ANOMALY_DETECTORS[anomaly_id]
        detected = detector_triggered(detectors, failed_tests)
        scored.append(
            {
                "anomaly_id": anomaly_id,
                "detected": detected,
                "detecting_tests": detectors,
                "target": item["target"],
            }
        )
    return scored


def run_dbt_test(condition_name: str, batch_id: str, db_path: Path) -> dict:
    cmd = [
        "dbt",
        "test",
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(PROFILES_DIR),
        "--target",
        "dev",
        "--no-partial-parse",
    ]

    env = os.environ.copy()
    env["DBT_DUCKDB_PATH"] = str(db_path).replace("\\", "/")

    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        env=env,
    )

    log_path = OUTPUT_DIR / f"{batch_id}__{condition_name}_dbt_test.log"
    log_path.write_text(proc.stdout + "\n" + proc.stderr, encoding="utf-8")

    failed_tests = parse_failed_tests_from_run_results()

    return {
        "condition": condition_name,
        "batch_id": batch_id,
        "returncode": proc.returncode,
        "failed_tests": failed_tests,
        "log_file": str(log_path),
        "stdout_tail": proc.stdout[-2000:],
        "stderr_tail": proc.stderr[-2000:],
    }


def inject_batch(db_path: Path, batch_id: str) -> list[dict]:
    con = duckdb.connect(str(db_path))
    manifest = []

    try:
        team_pairs = con.execute(
            """
            SELECT team_id, team_name
            FROM main.dim_teams
            ORDER BY team_id
            """
        ).fetchall()

        match_ids = [
            row[0]
            for row in con.execute(
                """
                SELECT match_id
                FROM main.fct_matches
                ORDER BY match_id
                """
            ).fetchall()
        ]

        if batch_id == "batch_A_key_integrity":
            t1, t2 = team_pairs[0], team_pairs[1]
            m1, m2 = match_ids[0], match_ids[1]

            con.execute(
                "UPDATE main.dim_teams SET team_id = NULL WHERE team_id = ?",
                [t2[0]],
            )
            manifest.append(
                {"id": "null_dim_teams_team_id", "target": f"dim_teams.team_id={t2[0]}"}
            )

            con.execute(
                "INSERT INTO main.dim_teams (team_id, team_name) VALUES (?, ?)",
                [t1[0], f"{t1[1]}_DUP"],
            )
            manifest.append(
                {
                    "id": "duplicate_dim_teams_team_id",
                    "target": f"dim_teams.team_id={t1[0]}",
                }
            )

            con.execute(
                "UPDATE main.fct_matches SET match_id = NULL WHERE match_id = ?",
                [m2],
            )
            manifest.append(
                {"id": "null_fct_matches_match_id", "target": f"fct_matches.match_id={m2}"}
            )

            con.execute(
                """
                INSERT INTO main.fct_matches
                SELECT *
                FROM main.fct_matches
                WHERE match_id = ?
                """,
                [m1],
            )
            manifest.append(
                {
                    "id": "duplicate_fct_matches_match_id",
                    "target": f"fct_matches.match_id={m1}",
                }
            )

        elif batch_id == "batch_B_semantic_domain":
            t3 = team_pairs[3]
            m3, m4, m5, m6, m7, _m8 = match_ids[10:16]

            con.execute(
                "UPDATE main.dim_teams SET team_name = NULL WHERE team_id = ?",
                [t3[0]],
            )
            manifest.append(
                {"id": "null_dim_teams_team_name", "target": f"dim_teams.team_id={t3[0]}"}
            )

            con.execute(
                "UPDATE main.fct_matches SET match_status = NULL WHERE match_id = ?",
                [m3],
            )
            manifest.append(
                {
                    "id": "null_fct_matches_match_status",
                    "target": f"fct_matches.match_id={m3}",
                }
            )

            con.execute(
                "UPDATE main.fct_matches SET match_status = 'BROKEN_STATUS' WHERE match_id = ?",
                [m4],
            )
            manifest.append(
                {
                    "id": "invalid_fct_matches_match_status",
                    "target": f"fct_matches.match_id={m4}",
                }
            )

            con.execute(
                "UPDATE main.fct_matches SET home_team_id = NULL WHERE match_id = ?",
                [m5],
            )
            manifest.append(
                {
                    "id": "null_fct_matches_home_team_id",
                    "target": f"fct_matches.match_id={m5}",
                }
            )

            con.execute(
                "UPDATE main.fct_matches SET away_team_id = NULL WHERE match_id = ?",
                [m6],
            )
            manifest.append(
                {
                    "id": "null_fct_matches_away_team_id",
                    "target": f"fct_matches.match_id={m6}",
                }
            )

            con.execute(
                "UPDATE main.fct_matches SET match_date = NULL WHERE match_id = ?",
                [m7],
            )
            manifest.append(
                {
                    "id": "null_fct_matches_match_date",
                    "target": f"fct_matches.match_id={m7}",
                }
            )

        elif batch_id == "batch_C_mixed_full_stack":
            t4, t5 = team_pairs[4], team_pairs[5]
            m9, m10, m11, _m12 = match_ids[20:24]

            con.execute(
                "UPDATE main.dim_teams SET team_id = NULL WHERE team_id = ?",
                [t5[0]],
            )
            manifest.append(
                {"id": "null_dim_teams_team_id", "target": f"dim_teams.team_id={t5[0]}"}
            )

            con.execute(
                "INSERT INTO main.dim_teams (team_id, team_name) VALUES (?, ?)",
                [t4[0], f"{t4[1]}_DUP2"],
            )
            manifest.append(
                {
                    "id": "duplicate_dim_teams_team_id",
                    "target": f"dim_teams.team_id={t4[0]}",
                }
            )

            con.execute(
                "UPDATE main.dim_teams SET team_name = NULL WHERE team_id = ?",
                [t4[0]],
            )
            manifest.append(
                {"id": "null_dim_teams_team_name", "target": f"dim_teams.team_id={t4[0]}"}
            )

            con.execute(
                """
                INSERT INTO main.fct_matches
                SELECT *
                FROM main.fct_matches
                WHERE match_id = ?
                """,
                [m9],
            )
            manifest.append(
                {
                    "id": "duplicate_fct_matches_match_id",
                    "target": f"fct_matches.match_id={m9}",
                }
            )

            con.execute(
                "UPDATE main.fct_matches SET home_team_id = NULL WHERE match_id = ?",
                [m10],
            )
            manifest.append(
                {
                    "id": "null_fct_matches_home_team_id",
                    "target": f"fct_matches.match_id={m10}",
                }
            )

            con.execute(
                "UPDATE main.fct_matches SET match_date = NULL WHERE match_id = ?",
                [m11],
            )
            manifest.append(
                {
                    "id": "null_fct_matches_match_date",
                    "target": f"fct_matches.match_id={m11}",
                }
            )

        else:
            raise ValueError(f"Unknown batch_id: {batch_id}")

        con.commit()

    finally:
        con.close()

    return manifest


def verify_injected_anomalies(db_path: Path, batch_manifest: list[dict]) -> dict[str, bool]:
    con = duckdb.connect(str(db_path))
    checks = {}

    try:
        allowed_statuses = (
            "'Match Finished', 'Match Postponed', 'Match Cancelled', "
            "'Match Scheduled', 'In Progress'"
        )

        for item in batch_manifest:
            anomaly_id = item["id"]

            if anomaly_id == "null_dim_teams_team_id":
                count = con.execute(
                    "SELECT COUNT(*) FROM main.dim_teams WHERE team_id IS NULL"
                ).fetchone()[0]
                checks[anomaly_id] = count > 0

            elif anomaly_id == "duplicate_dim_teams_team_id":
                count = con.execute(
                    """
                    SELECT COUNT(*)
                    FROM (
                        SELECT team_id
                        FROM main.dim_teams
                        WHERE team_id IS NOT NULL
                        GROUP BY team_id
                        HAVING COUNT(*) > 1
                    )
                    """
                ).fetchone()[0]
                checks[anomaly_id] = count > 0

            elif anomaly_id == "null_dim_teams_team_name":
                count = con.execute(
                    "SELECT COUNT(*) FROM main.dim_teams WHERE team_name IS NULL"
                ).fetchone()[0]
                checks[anomaly_id] = count > 0

            elif anomaly_id == "null_fct_matches_match_id":
                count = con.execute(
                    "SELECT COUNT(*) FROM main.fct_matches WHERE match_id IS NULL"
                ).fetchone()[0]
                checks[anomaly_id] = count > 0

            elif anomaly_id == "duplicate_fct_matches_match_id":
                count = con.execute(
                    """
                    SELECT COUNT(*)
                    FROM (
                        SELECT match_id
                        FROM main.fct_matches
                        WHERE match_id IS NOT NULL
                        GROUP BY match_id
                        HAVING COUNT(*) > 1
                    )
                    """
                ).fetchone()[0]
                checks[anomaly_id] = count > 0

            elif anomaly_id == "null_fct_matches_match_status":
                count = con.execute(
                    "SELECT COUNT(*) FROM main.fct_matches WHERE match_status IS NULL"
                ).fetchone()[0]
                checks[anomaly_id] = count > 0

            elif anomaly_id == "invalid_fct_matches_match_status":
                count = con.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM main.fct_matches
                    WHERE match_status IS NOT NULL
                      AND match_status NOT IN ({allowed_statuses})
                    """
                ).fetchone()[0]
                checks[anomaly_id] = count > 0

            elif anomaly_id == "null_fct_matches_home_team_id":
                count = con.execute(
                    "SELECT COUNT(*) FROM main.fct_matches WHERE home_team_id IS NULL"
                ).fetchone()[0]
                checks[anomaly_id] = count > 0

            elif anomaly_id == "null_fct_matches_away_team_id":
                count = con.execute(
                    "SELECT COUNT(*) FROM main.fct_matches WHERE away_team_id IS NULL"
                ).fetchone()[0]
                checks[anomaly_id] = count > 0

            elif anomaly_id == "null_fct_matches_match_date":
                count = con.execute(
                    "SELECT COUNT(*) FROM main.fct_matches WHERE match_date IS NULL"
                ).fetchone()[0]
                checks[anomaly_id] = count > 0

            else:
                checks[anomaly_id] = False

    finally:
        con.close()

    return checks


def run_condition_for_batch(batch_id: str, condition_name: str, schema_source: Path) -> dict:
    temp_db = get_temp_db_path(condition_name, batch_id)

    restore_clean_db(temp_db)
    restore_schema(schema_source)

    batch_manifest = inject_batch(temp_db, batch_id)
    mutation_checks = verify_injected_anomalies(temp_db, batch_manifest)
    run_info = run_dbt_test(condition_name, batch_id, temp_db)
    detection = score_detection(batch_manifest, run_info["failed_tests"])

    detected_count = sum(1 for d in detection if d["detected"])
    total_anomalies = len(detection)

    result = {
        "batch_id": batch_id,
        "condition": condition_name,
        "schema_source": str(schema_source),
        "db_path_used": str(temp_db),
        "returncode": run_info["returncode"],
        "failed_tests": run_info["failed_tests"],
        "log_file": run_info["log_file"],
        "mutation_verification": mutation_checks,
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
    print(f"[{batch_id}][{condition_name}] mutation checks: {mutation_checks}")
    print(f"[{batch_id}][{condition_name}] db path: {temp_db}")
    print(f"[{batch_id}][{condition_name}] summary: {out_path}")

    return result


def summarize_condition_results(results: list[dict]) -> dict:
    total_detected = sum(r["detected_count"] for r in results)
    total_anomalies = sum(r["total_anomalies"] for r in results)
    avg_rate = (
        round(sum(r["detection_rate"] for r in results) / len(results), 4)
        if results
        else None
    )

    return {
        "total_detected": total_detected,
        "total_anomalies": total_anomalies,
        "overall_detection_rate": round(total_detected / total_anomalies, 4)
        if total_anomalies
        else None,
        "average_batch_detection_rate": avg_rate,
        "batch_count": len(results),
    }


def compute_gain_summary(baseline: dict, comparator: dict) -> dict:
    baseline_detected = baseline["total_detected"]
    comparator_detected = comparator["total_detected"]

    return {
        "absolute_additional_detections": comparator_detected - baseline_detected,
        "absolute_detection_rate_gain": round(
            comparator["overall_detection_rate"] - baseline["overall_detection_rate"],
            4,
        )
        if baseline["overall_detection_rate"] is not None
        else None,
        "relative_detection_count_improvement": round(
            ((comparator_detected - baseline_detected) / baseline_detected) * 100,
            2,
        )
        if baseline_detected > 0
        else None,
    }


def main() -> None:
    load_env_file(ENV_FILE)
    required_env = ["AWS_S3_BUCKET_NAME"]
    missing_env = [k for k in required_env if not os.environ.get(k)]
    if missing_env:
        raise RuntimeError(f"Missing required environment variables: {missing_env}")    
    schema_map = ensure_prereqs()

    condition_results: dict[str, list[dict]] = {
        "manual_only": [],
        "manual_expanded": [],
        "manual_plus_llm": [],
    }

    try:
        for batch in BATCHES:
            batch_id = batch["batch_id"]

            for condition_name, schema_source in schema_map.items():
                condition_results[condition_name].append(
                    run_condition_for_batch(batch_id, condition_name, schema_source)
                )

        aggregate = {
            condition_name: {
                "per_batch_results": results,
                "aggregate": summarize_condition_results(results),
            }
            for condition_name, results in condition_results.items()
        }

        final_summary = {
            "batches": BATCHES,
            "manual_only": aggregate["manual_only"],
            "manual_expanded": aggregate["manual_expanded"],
            "manual_plus_llm": aggregate["manual_plus_llm"],
            "improvement_vs_manual_only": {
                "manual_expanded": compute_gain_summary(
                    aggregate["manual_only"]["aggregate"],
                    aggregate["manual_expanded"]["aggregate"],
                ),
                "manual_plus_llm": compute_gain_summary(
                    aggregate["manual_only"]["aggregate"],
                    aggregate["manual_plus_llm"]["aggregate"],
                ),
            },
        }

        final_path = OUTPUT_DIR / "final_summary.json"
        final_path.write_text(json.dumps(final_summary, indent=2), encoding="utf-8")
        print(f"\nSaved final summary to {final_path}")

    finally:
        restore_schema(schema_map["manual_only"])
        if TARGET_DB.exists() and CLEAN_DB.exists():
            shutil.copyfile(CLEAN_DB, TARGET_DB)


if __name__ == "__main__":
    main()