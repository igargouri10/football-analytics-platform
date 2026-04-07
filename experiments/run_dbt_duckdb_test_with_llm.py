import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from experiments.runtime_logger import append_stage_result, utc_now_iso

DBT_DIR = ROOT / "dbt_project"
LOG_DIR = ROOT / "experiments" / "runtime_logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def parse_dbt_summary(text: str) -> dict:
    summary = {}

    match = re.search(
        r"Done\.\s+PASS=(\d+)\s+WARN=(\d+)\s+ERROR=(\d+)\s+SKIP=(\d+)\s+NO-OP=(\d+)\s+TOTAL=(\d+)",
        text,
        re.MULTILINE,
    )
    if match:
        summary.update({
            "pass": int(match.group(1)),
            "warn": int(match.group(2)),
            "error": int(match.group(3)),
            "skip": int(match.group(4)),
            "no_op": int(match.group(5)),
            "total": int(match.group(6)),
        })

    run_match = re.search(
        r"Finished running .* in .* \(([\d\.]+)s\)\.",
        text,
        re.MULTILINE,
    )
    if run_match:
        summary["dbt_reported_duration_seconds"] = float(run_match.group(1))

    found_match = re.search(
        r"Found\s+(\d+)\s+models,\s+(\d+)\s+data tests,\s+(\d+)\s+source",
        text,
        re.MULTILINE,
    )
    if found_match:
        summary["models_found"] = int(found_match.group(1))
        summary["data_tests_found"] = int(found_match.group(2))
        summary["sources_found"] = int(found_match.group(3))

    return summary


def main():
    start_time = utc_now_iso()

    cmd = [
        "dbt",
        "test",
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(DBT_DIR),
        "--target",
        "prod",
        "--no-partial-parse",
    ]

    proc = subprocess.run(
        cmd,
        cwd=str(DBT_DIR),
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )

    combined_output = proc.stdout + "\n" + proc.stderr
    log_path = LOG_DIR / "dbt_test_duckdb_with_llm.log"
    log_path.write_text(combined_output, encoding="utf-8")

    summary = parse_dbt_summary(combined_output)
    status = "success" if proc.returncode == 0 else "failed"

    append_stage_result(
        stage_name="dbt_test_duckdb_with_llm",
        start_time=start_time,
        end_time=utc_now_iso(),
        status=status,
        metadata={
            "command": " ".join(cmd),
            "returncode": proc.returncode,
            "log_file": str(log_path),
            **summary,
        },
    )

    print(combined_output)

    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


if __name__ == "__main__":
    main()