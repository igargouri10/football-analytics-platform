import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from experiments.runtime_logger import append_stage_result, utc_now_iso

LOG_DIR = ROOT / "experiments" / "runtime_logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

AUDIT_SUMMARY = ROOT / "experiments" / "usefulness_audit" / "generated_test_usefulness_summary.json"


def main():
    start_time = utc_now_iso()

    cmd = [sys.executable, str(ROOT / "experiments" / "audit_generated_test_usefulness.py")]

    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )

    combined_output = proc.stdout + "\n" + proc.stderr
    log_path = LOG_DIR / "run_generated_test_usefulness_audit.log"
    log_path.write_text(combined_output, encoding="utf-8")

    metadata = {
        "command": " ".join(cmd),
        "returncode": proc.returncode,
        "log_file": str(log_path),
    }

    if AUDIT_SUMMARY.exists():
        try:
            summary = json.loads(AUDIT_SUMMARY.read_text(encoding="utf-8"))
            audit = summary["test_audit_summary"]
            metadata.update({
                "summary_file": str(AUDIT_SUMMARY),
                "total_generated_test_items": audit["total_generated_test_items"],
                "useful_count": audit["useful_count"],
                "redundant_count": audit["redundant_count"],
                "executable_but_low_value_count": audit["executable_but_low_value_count"],
                "invalid_or_non_executable_count": audit["invalid_or_non_executable_count"],
                "useful_percentage_of_all_generated": audit["useful_percentage_of_all_generated"],
                "useful_percentage_of_nonredundant_executable": audit["useful_percentage_of_nonredundant_executable"],
            })
        except Exception as e:
            metadata["summary_parse_error"] = str(e)

    append_stage_result(
        stage_name="run_generated_test_usefulness_audit",
        start_time=start_time,
        end_time=utc_now_iso(),
        status="success" if proc.returncode == 0 else "failed",
        metadata=metadata,
    )

    print(combined_output)

    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


if __name__ == "__main__":
    main()