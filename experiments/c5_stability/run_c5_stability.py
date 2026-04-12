from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
EXPERIMENTS_DIR = ROOT / "experiments"
C5_DIR = EXPERIMENTS_DIR / "c5_stability"
FROZEN_DIR = C5_DIR / "frozen_schema"
FRESH_DIR = C5_DIR / "fresh_generation"

PYTHON_EXE = sys.executable
DBT_EXE_CANDIDATE = Path(PYTHON_EXE).with_name("dbt.exe")
DBT_EXE = str(DBT_EXE_CANDIDATE) if DBT_EXE_CANDIDATE.exists() else "dbt"

DBT_DIR = ROOT / "dbt_project"
GENERATED_TESTS_DIR = DBT_DIR / "generated_tests"
SCHEMA_VERSIONS_DIR = DBT_DIR / "schema_versions"
MARTS_DIR = DBT_DIR / "models" / "marts"

ACTIVE_SCHEMA = MARTS_DIR / "schema.yml"
MANUAL_BASELINE_SCHEMA = SCHEMA_VERSIONS_DIR / "schema.manual_baseline.yml"
MANUAL_EXPANDED_SCHEMA = SCHEMA_VERSIONS_DIR / "schema.manual_expanded.yml"
NORMALIZED_LLM_SCHEMA = SCHEMA_VERSIONS_DIR / "schema.llm_merged.yml"

LIVE_DUCKDB_PATH = DBT_DIR / "target" / "dbt.duckdb"
CLEAN_DUCKDB_PATH = DBT_DIR / "target" / "dbt.clean.duckdb"

LLM_SCHEMA_CANDIDATES = [
    NORMALIZED_LLM_SCHEMA,
    MARTS_DIR / "schema.llm_merged.yml.txt",
]

GENERATION_SUMMARY_CANDIDATES = [
    GENERATED_TESTS_DIR / "generation_summary.json",
]

USEFULNESS_SUMMARY_CANDIDATES = [
    EXPERIMENTS_DIR / "usefulness_audit" / "generated_test_usefulness_summary.json",
]

COMPARATOR_FINAL_SUMMARY = (
    EXPERIMENTS_DIR / "multibatch_anomaly_results" / "final_summary.json"
)

ENV_FILE = ROOT / ".env"
PROFILES_DIR = Path.home() / ".dbt"

FROZEN_RUNS = 5
FRESH_TRIALS = 5

GENERATE_COMMAND = [PYTHON_EXE, "-m", "llm_tests.generate_dbt_tests"]
MERGE_COMMAND = None
USEFULNESS_AUDIT_COMMAND = [PYTHON_EXE, "-m", "experiments.run_generated_test_usefulness_audit"]

RUN_CLEAN_DBT_TEST = True
DBT_TEST_COMMAND = [
    DBT_EXE,
    "test",
    "--project-dir",
    "dbt_project",
    "--profiles-dir",
    str(PROFILES_DIR),
]

DELETE_STALE_LLM_OUTPUTS = True


def ensure_dirs() -> None:
    for path in [C5_DIR, FROZEN_DIR, FRESH_DIR]:
        path.mkdir(parents=True, exist_ok=True)


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
        if name:
            os.environ[name] = value


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def run_command(
    cmd: list[str],
    log_path: Path,
    env: dict[str, str] | None = None,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess:
    if cwd is None:
        cwd = ROOT
    if env is None:
        env = os.environ.copy()

    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
    )

    log_path.write_text(
        f"$ {' '.join(map(str, cmd))}\n\nSTDOUT:\n{proc.stdout}\n\nSTDERR:\n{proc.stderr}",
        encoding="utf-8",
    )

    if proc.returncode != 0:
        raise RuntimeError(
            f"Command failed with return code {proc.returncode}: {' '.join(map(str, cmd))}\n"
            f"See log: {log_path}"
        )

    return proc


def copy_manual_baseline_to_active() -> None:
    shutil.copyfile(MANUAL_BASELINE_SCHEMA, ACTIVE_SCHEMA)


def delete_if_exists(path: Path) -> None:
    if path.exists():
        if path.is_file():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)


def cleanup_stale_llm_outputs() -> None:
    # Do not delete NORMALIZED_LLM_SCHEMA.
    # Frozen-schema runs depend on schema_versions/schema.llm_merged.yml
    # remaining available across reruns.
    delete_if_exists(MARTS_DIR / "schema.llm_merged.yml.txt")

    if GENERATED_TESTS_DIR.exists():
        for item in GENERATED_TESTS_DIR.iterdir():
            if item.is_file() and (
                item.suffix in {".yml", ".yaml", ".json"} or item.name.endswith(".txt")
            ):
                item.unlink()


def restore_llm_schema_from_previous_snapshot() -> Path | None:
    snapshots = sorted(FROZEN_DIR.glob("run_*/schema.llm_merged.snapshot.yml"))
    if not snapshots:
        return None

    latest_snapshot = snapshots[-1]
    NORMALIZED_LLM_SCHEMA.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(latest_snapshot, NORMALIZED_LLM_SCHEMA)
    return NORMALIZED_LLM_SCHEMA


def resolve_existing_llm_schema() -> Path:
    for candidate in LLM_SCHEMA_CANDIDATES:
        if candidate.exists():
            return candidate

    restored = restore_llm_schema_from_previous_snapshot()
    if restored and restored.exists():
        return restored

    raise FileNotFoundError(
        "Could not find an LLM merged schema in any expected location:\n"
        + "\n".join(str(p) for p in LLM_SCHEMA_CANDIDATES)
    )


def normalize_llm_schema() -> Path:
    src = resolve_existing_llm_schema()
    if src.resolve() != NORMALIZED_LLM_SCHEMA.resolve():
        NORMALIZED_LLM_SCHEMA.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, NORMALIZED_LLM_SCHEMA)
    return NORMALIZED_LLM_SCHEMA


def resolve_generation_source_db() -> Path:
    if CLEAN_DUCKDB_PATH.exists():
        return CLEAN_DUCKDB_PATH
    if LIVE_DUCKDB_PATH.exists():
        return LIVE_DUCKDB_PATH
    raise FileNotFoundError(
        "Could not find a DuckDB source database for generation. Checked:\n"
        f"{CLEAN_DUCKDB_PATH}\n{LIVE_DUCKDB_PATH}"
    )


def copy_if_exists(src_candidates: list[Path], dst: Path) -> bool:
    for src in src_candidates:
        if src.exists():
            shutil.copyfile(src, dst)
            return True
    return False


def write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def archive_common_artifacts(trial_dir: Path) -> None:
    if COMPARATOR_FINAL_SUMMARY.exists():
        shutil.copyfile(
            COMPARATOR_FINAL_SUMMARY,
            trial_dir / "comparator_final_summary.json",
        )

    try:
        llm_schema = resolve_existing_llm_schema()
        shutil.copyfile(llm_schema, trial_dir / "schema.llm_merged.snapshot.yml")
    except FileNotFoundError:
        pass

    copy_if_exists(
        GENERATION_SUMMARY_CANDIDATES,
        trial_dir / "generation_summary.json",
    )

    copy_if_exists(
        USEFULNESS_SUMMARY_CANDIDATES,
        trial_dir / "generated_test_usefulness_summary.json",
    )


def run_frozen_schema_once(run_idx: int) -> None:
    run_dir = FROZEN_DIR / f"run_{run_idx:02d}"
    run_dir.mkdir(parents=True, exist_ok=True)

    started = time.time()

    manifest = {
        "mode": "frozen_schema",
        "run_index": run_idx,
        "started_at": now_str(),
        "commands": [],
    }

    llm_schema = normalize_llm_schema()
    manifest["llm_schema_used"] = str(llm_schema)

    log_path = run_dir / "01_comparator.log"
    cmd = [PYTHON_EXE, "-m", "experiments.run_multibatch_anomaly_experiment"]
    manifest["commands"].append(
        {
            "name": "comparator",
            "cmd": cmd,
            "log": str(log_path),
        }
    )
    run_command(cmd, log_path)

    archive_common_artifacts(run_dir)

    manifest["finished_at"] = now_str()
    manifest["duration_seconds"] = round(time.time() - started, 3)
    write_manifest(run_dir / "trial_manifest.json", manifest)


def run_fresh_generation_once(trial_idx: int) -> None:
    trial_dir = FRESH_DIR / f"trial_{trial_idx:02d}"
    trial_dir.mkdir(parents=True, exist_ok=True)

    started = time.time()

    manifest = {
        "mode": "fresh_generation",
        "trial_index": trial_idx,
        "started_at": now_str(),
        "commands": [],
    }

    copy_manual_baseline_to_active()

    if DELETE_STALE_LLM_OUTPUTS:
        cleanup_stale_llm_outputs()

    generation_source_db = resolve_generation_source_db()
    generation_db_copy = trial_dir / "generation_source.duckdb"
    shutil.copyfile(generation_source_db, generation_db_copy)

    generation_env = os.environ.copy()
    generation_env["DBT_DUCKDB_PATH"] = str(generation_db_copy)

    log_path = trial_dir / "01_generate.log"
    manifest["commands"].append(
        {
            "name": "generate",
            "cmd": GENERATE_COMMAND,
            "log": str(log_path),
            "duckdb_path": str(generation_db_copy),
        }
    )
    run_command(GENERATE_COMMAND, log_path, env=generation_env)

    if MERGE_COMMAND is not None:
        log_path = trial_dir / "02_merge.log"
        manifest["commands"].append(
            {"name": "merge", "cmd": MERGE_COMMAND, "log": str(log_path)}
        )
        run_command(MERGE_COMMAND, log_path)

    llm_schema = normalize_llm_schema()
    manifest["llm_schema_used"] = str(llm_schema)

    if RUN_CLEAN_DBT_TEST:
        log_path = trial_dir / "03_clean_dbt_test.log"
        manifest["commands"].append(
            {
                "name": "dbt_test_after_merge",
                "cmd": DBT_TEST_COMMAND,
                "log": str(log_path),
                "duckdb_path": str(generation_db_copy),
            }
        )
        run_command(DBT_TEST_COMMAND, log_path, env=generation_env)

    log_path = trial_dir / "04_comparator.log"
    comparator_cmd = [PYTHON_EXE, "-m", "experiments.run_multibatch_anomaly_experiment"]
    manifest["commands"].append(
        {
            "name": "comparator",
            "cmd": comparator_cmd,
            "log": str(log_path),
        }
    )
    run_command(comparator_cmd, log_path)

    if USEFULNESS_AUDIT_COMMAND is not None:
        log_path = trial_dir / "05_usefulness_audit.log"
        manifest["commands"].append(
            {"name": "usefulness_audit", "cmd": USEFULNESS_AUDIT_COMMAND, "log": str(log_path)}
        )
        run_command(USEFULNESS_AUDIT_COMMAND, log_path)

    archive_common_artifacts(trial_dir)

    manifest["finished_at"] = now_str()
    manifest["duration_seconds"] = round(time.time() - started, 3)
    write_manifest(trial_dir / "trial_manifest.json", manifest)


def main() -> None:
    ensure_dirs()
    load_env_file(ENV_FILE)

    if not MANUAL_BASELINE_SCHEMA.exists():
        raise FileNotFoundError(f"Missing baseline schema: {MANUAL_BASELINE_SCHEMA}")
    if not MANUAL_EXPANDED_SCHEMA.exists():
        raise FileNotFoundError(f"Missing expanded schema: {MANUAL_EXPANDED_SCHEMA}")

    for run_idx in range(1, FROZEN_RUNS + 1):
        print(f"\n=== Frozen-schema run {run_idx}/{FROZEN_RUNS} ===")
        run_frozen_schema_once(run_idx)

    for trial_idx in range(1, FRESH_TRIALS + 1):
        print(f"\n=== Fresh-generation trial {trial_idx}/{FRESH_TRIALS} ===")
        run_fresh_generation_once(trial_idx)

    copy_manual_baseline_to_active()
    print("\nC5 stability runs completed successfully.")


if __name__ == "__main__":
    main()