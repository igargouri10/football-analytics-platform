import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DIR = ROOT / "experiments" / "runtime_logs"
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
RUNTIME_FILE = RUNTIME_DIR / "latest_pipeline_runtime.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_payload() -> dict:
    return {"stages": []}


def load_runtime_log() -> dict:
    if not RUNTIME_FILE.exists():
        return _default_payload()

    try:
        return json.loads(RUNTIME_FILE.read_text(encoding="utf-8"))
    except Exception:
        return _default_payload()


def write_runtime_log(data: dict) -> None:
    RUNTIME_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def reset_runtime_log() -> None:
    write_runtime_log(_default_payload())


def append_stage_result(
    stage_name: str,
    start_time: str,
    end_time: str,
    status: str,
    metadata: dict | None = None,
) -> None:
    data = load_runtime_log()

    start_dt = datetime.fromisoformat(start_time)
    end_dt = datetime.fromisoformat(end_time)

    data["stages"].append({
        "stage_name": stage_name,
        "start_time_utc": start_time,
        "end_time_utc": end_time,
        "duration_seconds": round((end_dt - start_dt).total_seconds(), 3),
        "status": status,
        "metadata": metadata or {},
    })

    write_runtime_log(data)