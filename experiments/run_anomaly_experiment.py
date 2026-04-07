import json
import os
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

OUTPUT_DIR = ROOT / "experiments" / "anomaly_results"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ANOMALY_SPECS = [
    {
        "id": "null_dim_teams_team_id",
        "detecting_tests": ["not_null_dim_teams_team_id"],
    },
    {
        "id": "duplicate_dim_teams_team_id",
        "detecting_tests": ["unique_dim_teams_team_id"],
    },
    {
        "id": "null_dim_teams_team_name",
        "detecting_tests": ["not_null_dim_teams_team_name"],
    },
    {
        "id": "null_fct_matches_match_id",
        "detecting_tests": ["not_null_fct_matches_match_id"],
    },
    {
        "id": "duplicate_fct_matches_match_id",
        "detecting_tests": ["unique_fct_matches_match_id"],
    },
    {
        "id": "null_fct_matches_match_status",
        "detecting_tests": ["not_null_fct_matches_match_status"],
    },
    {
        "id": "invalid_fct_matches_match_status",
        "detecting_tests": ["accepted_values_fct_matches_match_status"],
    },
    {
        "id": "null_fct_matches_home_team_id",
        "detecting_tests": [
            "not_null_fct_matches_home_team_id",
            "not_null_fct_training_dataset_home_team_id",
        ],
    },
    {
        "id": "null_fct_matches_away_team_id",
        "detecting_tests": [
            "not_null_fct_matches_away_team_id",
            "not_null_fct_training_dataset_away_team_id",
        ],
    },
    {
        "id": "null_fct_matches_match_date",
        "detecting_tests": [
            "not_null_fct_matches_match_date",
            "not_null_fct_training_dataset_match_date",
        ],
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

    required_env = ["AWS_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
    missing_env = [k for k in required_env if not os.getenv(k)]
    if missing_env:
        raise EnvironmentError(
            "Missing required environment variables in current shell: "
            + ", ".join(missing_env)
        )


def restore_clean_db():
    shutil.copy2(CLEAN_DB, TARGET_DB)


def restore_schema(schema_source: Path):
    shutil.copy2(schema_source, ACTIVE_SCHEMA)


def inject_anomalies(db_path: Path):
    con = duckdb.connect(str(db_path))
    manifest = []

    try:
        team_rows = con.execute("""
            SELECT team_id, team_name
            FROM main.dim_teams
            ORDER BY team_id
            LIMIT 3
        """).fetchall()

        match_rows = con.execute("""
            SELECT match_id
            FROM main.fct_matches
            ORDER BY match_id
            LIMIT 7
        """).fetchall()

        # 1) null_dim_teams_team_id
        team_id_2 = team_rows[1][0]
        con.execute(
            "UPDATE main.dim_teams SET team_id = NULL WHERE team_id = ?",
            [team_id_2],
        )
        manifest.append({"id": "null_dim_teams_team_id", "target": f"dim_teams.team_id={team_id_2}"})

        # 2) duplicate_dim_teams_team_id
        dup_team_id = team_rows[0][0]
        dup_team_name = team_rows[0][1]
        con.execute("""
            INSERT INTO main.dim_teams (team_id, team_name)
            VALUES (?, ?)
        """, [dup_team_id, f"{dup_team_name}_DUP"])
        manifest.append({"id": "duplicate_dim_teams_team_id", "target": f"dim_teams.team_id={dup_team_id}"})

        # 3) null_dim_teams_team_name
        team_id_3 = team_rows[2][0]
        con.execute(
            "UPDATE main.dim_teams SET team_name = NULL WHERE team_id = ?",
            [team_id_3],
        )
        manifest.append({"id": "null_dim_teams_team_name", "target": f"dim_teams.team_id={team_id_3}"})

        # Pick 7 distinct matches so each anomaly is isolated
        m1 = match_rows[0][0]
        m2 = match_rows[1][0]
        m3 = match_rows[2][0]
        m4 = match_rows[3][0]
        m5 = match_rows[4][0]
        m6 = match_rows[5][0]
        m7 = match_rows[6][0]

        # 4) null_fct_matches_match_id
        con.execute(
            "UPDATE main.fct_matches SET match_id = NULL WHERE match_id = ?",
            [m2],
        )
        manifest.append({"id": "null_fct_matches_match_id", "target": f"fct_matches.match_id={m2}"})

        # 5) duplicate_fct_matches_match_id
        con.execute("""
            INSERT INTO main.fct_matches
            SELECT *
            FROM main.fct_matches
            WHERE match_id = ?
        """, [m1])
        manifest.append({"id": "duplicate_fct_matches_match_id", "target": f"fct_matches.match_id={m1}"})

        # 6) null_fct_matches_match_status
        con.execute(
            "UPDATE main.fct_matches SET match_status = NULL WHERE match_id = ?",
            [m3],
        )
        manifest.append({"id": "null_fct_matches_match_status", "target": f"fct_matches.match_id={m3}"})

        # 7) invalid_fct_matches_match_status
        con.execute(
            "UPDATE main.fct_matches SET match_status = 'BROKEN_STATUS' WHERE match_id = ?",
            [m4],
        )
        manifest.append({"id": "invalid_fct_matches_match_status", "target": f"fct_matches.match_id={m4}"})

        # 8) null_fct_matches_home_team_id
        con.execute(
            "UPDATE main.fct_matches SET home_team_id = NULL WHERE match_id = ?",
            [m5],
        )
        manifest.append({"id": "null_fct_matches_home_team_id", "target": f"fct_matches.match_id={m5}"})

        # 9) null_fct_matches_away_team_id
        con.execute(
            "UPDATE main.fct_matches SET away_team_id = NULL WHERE match_id = ?",
            [m6],
        )
        manifest.append({"id": "null_fct_matches_away_team_id", "target": f"fct_matches.match_id={m6}"})

        # 10) null_fct_matches_match_date
        con.execute(
            "UPDATE main.fct_matches SET match_date = NULL WHERE match_id = ?",
            [m7],
        )
        manifest.append({"id": "null_fct_matches_match_date", "target": f"fct_matches.match_id={m7}"})

    finally:
        con.close()

    return manifest


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

        # keep only likely test-name tokens
        if (
            candidate not in failed
            and candidate not in {"in", "of", "[FAIL"}
        ):
            failed.append(candidate)

    return failed


def score_detection(failed_tests):
    scored = []
    for spec in ANOMALY_SPECS:
        detected = any(
            any(detector in failed for detector in spec["detecting_tests"])
            for failed in failed_tests
        )
        scored.append({
            "anomaly_id": spec["id"],
            "detected": detected,
            "detecting_tests": spec["detecting_tests"],
        })
    return scored


def run_dbt_test(condition_name: str):
    cmd = ["dbt", "test", "--target", "dev", "--no-partial-parse"]
    proc = subprocess.run(
        cmd,
        cwd=str(DBT_DIR),
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )

    log_path = OUTPUT_DIR / f"{condition_name}_dbt_test.log"
    log_path.write_text(proc.stdout + "\n" + proc.stderr, encoding="utf-8")

    failed_tests = parse_failed_tests(proc.stdout)
    return {
        "condition": condition_name,
        "returncode": proc.returncode,
        "failed_tests": failed_tests,
        "log_file": str(log_path),
    }


def run_condition(condition_name: str, schema_source: Path):
    restore_clean_db()
    restore_schema(schema_source)
    manifest = inject_anomalies(TARGET_DB)
    run_info = run_dbt_test(condition_name)
    detection = score_detection(run_info["failed_tests"])

    result = {
        "condition": condition_name,
        "schema_source": str(schema_source),
        "anomaly_manifest": manifest,
        "failed_tests": run_info["failed_tests"],
        "returncode": run_info["returncode"],
        "detected_count": sum(1 for d in detection if d["detected"]),
        "total_anomalies": len(detection),
        "detection_results": detection,
        "log_file": run_info["log_file"],
    }

    out_path = OUTPUT_DIR / f"{condition_name}_summary.json"
    out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"[{condition_name}] detected {result['detected_count']}/{result['total_anomalies']} anomalies")
    print(f"[{condition_name}] failed tests: {result['failed_tests']}")
    print(f"[{condition_name}] summary: {out_path}")
    return result


def main():
    ensure_prereqs()

    try:
        manual_result = run_condition("manual_only", MANUAL_SCHEMA)
        llm_result = run_condition("manual_plus_llm", LLM_SCHEMA)

        final_summary = {
            "manual_only": manual_result,
            "manual_plus_llm": llm_result,
        }

        final_path = OUTPUT_DIR / "final_summary.json"
        final_path.write_text(json.dumps(final_summary, indent=2), encoding="utf-8")
        print(f"\nSaved final summary to {final_path}")

    finally:
        restore_clean_db()
        restore_schema(LLM_SCHEMA if LLM_SCHEMA.exists() else ACTIVE_SCHEMA)


if __name__ == "__main__":
    main()