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

MULTIBATCH_SUMMARY = ROOT / "experiments" / "multibatch_anomaly_results" / "final_summary.json"


def main():
    start_time = utc_now_iso()

    cmd = [sys.executable, str(ROOT / "experiments" / "run_multibatch_anomaly_experiment.py")]

    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )

    combined_output = proc.stdout + "\n" + proc.stderr
    log_path = LOG_DIR / "run_multibatch_anomaly_experiment.log"
    log_path.write_text(combined_output, encoding="utf-8")

    metadata = {
        "command": " ".join(cmd),
        "returncode": proc.returncode,
        "log_file": str(log_path),
    }

    if MULTIBATCH_SUMMARY.exists():
        try:
            summary = json.loads(MULTIBATCH_SUMMARY.read_text(encoding="utf-8"))
            metadata.update({
                "summary_file": str(MULTIBATCH_SUMMARY),
                "manual_total_detected": summary["manual_only"]["aggregate"]["total_detected"],
                "manual_total_anomalies": summary["manual_only"]["aggregate"]["total_anomalies"],
                "manual_overall_detection_rate": summary["manual_only"]["aggregate"]["overall_detection_rate"],
                "llm_total_detected": summary["manual_plus_llm"]["aggregate"]["total_detected"],
                "llm_total_anomalies": summary["manual_plus_llm"]["aggregate"]["total_anomalies"],
                "llm_overall_detection_rate": summary["manual_plus_llm"]["aggregate"]["overall_detection_rate"],
                "absolute_additional_detections": summary["improvement_summary"]["absolute_additional_detections"],
                "relative_detection_count_improvement": summary["improvement_summary"]["relative_detection_count_improvement"],
            })
        except Exception as e:
            metadata["summary_parse_error"] = str(e)

    append_stage_result(
        stage_name="run_multibatch_anomaly_experiment",
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