import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DIR = ROOT / "experiments" / "runtime_logs"
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
RUNTIME_FILE = RUNTIME_DIR / "latest_pipeline_runtime.json"


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def load_runtime_log():
    if RUNTIME_FILE.exists():
        return json.loads(RUNTIME_FILE.read_text(encoding="utf-8"))
    return {"stages": []}


def write_runtime_log(data):
    RUNTIME_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")

def reset_runtime_log():
    write_runtime_log({"stages": []})


def append_stage_result(stage_name, start_time, end_time, status, metadata=None):
    data = load_runtime_log()
    data["stages"].append({
        "stage_name": stage_name,
        "start_time_utc": start_time,
        "end_time_utc": end_time,
        "duration_seconds": round(
            datetime.fromisoformat(end_time) - datetime.fromisoformat(start_time)
        ).total_seconds() if False else None,
        "status": status,
        "metadata": metadata or {},
    })

    # safer duration calculation
    start_dt = datetime.fromisoformat(start_time)
    end_dt = datetime.fromisoformat(end_time)
    data["stages"][-1]["duration_seconds"] = round((end_dt - start_dt).total_seconds(), 3)

    write_runtime_log(data)