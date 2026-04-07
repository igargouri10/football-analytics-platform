import json
import hashlib
import os
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
import sys
from pathlib import Path

import duckdb
import snowflake.connector

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from experiments.runtime_logger import append_stage_result, utc_now_iso

DUCKDB_PATH = ROOT / "dbt_project" / "target" / "dbt.duckdb"

TABLE_CONFIG = {
    "dim_teams": {
        "duckdb_table": "main.dim_teams",
        "snowflake_table": "PROD.DUCKDB_MIGRATED.DIM_TEAMS",
        "columns": ["team_id", "team_name"],
        "duckdb_order_by": "team_id",
        "snowflake_order_by": "TEAM_ID",
    },
    "fct_matches": {
        "duckdb_table": "main.fct_matches",
        "snowflake_table": "PROD.DUCKDB_MIGRATED.FCT_MATCHES",
        "columns": [
            "match_id", "league_id", "season", "match_date", "match_status",
            "home_team_id", "home_team_name", "home_team_score",
            "away_team_id", "away_team_name", "away_team_score",
        ],
        "duckdb_order_by": "match_id",
        "snowflake_order_by": "MATCH_ID",
    },
    "fct_training_dataset": {
        "duckdb_table": "main.fct_training_dataset",
        "snowflake_table": "PROD.DUCKDB_MIGRATED.FCT_TRAINING_DATASET",
        "columns": [
            "match_id", "match_date", "match_result",
            "home_team_id", "home_team_name",
            "home_avg_goals_scored_last_5", "home_avg_goals_conceded_last_5",
            "away_team_id", "away_team_name",
            "away_avg_goals_scored_last_5", "away_avg_goals_conceded_last_5",
        ],
        "duckdb_order_by": "match_id",
        "snowflake_order_by": "MATCH_ID",
    },
}

NUMERIC_TOLERANCE_COLS = {
    "home_avg_goals_scored_last_5",
    "home_avg_goals_conceded_last_5",
    "away_avg_goals_scored_last_5",
    "away_avg_goals_conceded_last_5",
}


def normalize_value(v):
    if v is None:
        return None
    if isinstance(v, (date, datetime, time)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v


def normalize_row(row):
    return [normalize_value(v) for v in row]


def checksum_rows(rows):
    payload = json.dumps([normalize_row(r) for r in rows], sort_keys=True, ensure_ascii=False)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def values_equal(a, b, col):
    a = normalize_value(a)
    b = normalize_value(b)

    if col in NUMERIC_TOLERANCE_COLS:
        if a is None and b is None:
            return True
        if a is None or b is None:
            return False
        return abs(float(a) - float(b)) <= 0.0011

    return a == b


def compare_table(duck_con, snow_cur, logical_name, cfg):
    duck_table = cfg["duckdb_table"]
    snow_table = cfg["snowflake_table"]
    cols = cfg["columns"]

    duck_cols_sql = ", ".join(cols)
    snow_cols_sql = ", ".join(c.upper() for c in cols)

    duck_count = duck_con.execute(f"SELECT COUNT(*) FROM {duck_table}").fetchone()[0]
    snow_cur.execute(f"SELECT COUNT(*) FROM {snow_table}")
    snow_count = snow_cur.fetchone()[0]

    duck_rows = duck_con.execute(
        f"SELECT {duck_cols_sql} FROM {duck_table} ORDER BY {cfg['duckdb_order_by']}"
    ).fetchall()

    snow_cur.execute(
        f"SELECT {snow_cols_sql} FROM {snow_table} ORDER BY {cfg['snowflake_order_by']}"
    )
    snow_rows = snow_cur.fetchall()

    raw_checksum_match = checksum_rows(duck_rows) == checksum_rows(snow_rows)

    null_summary = {}
    for col in cols:
        duck_nulls = duck_con.execute(
            f"SELECT COUNT(*) FROM {duck_table} WHERE {col} IS NULL"
        ).fetchone()[0]

        snow_cur.execute(
            f"SELECT COUNT(*) FROM {snow_table} WHERE {col.upper()} IS NULL"
        )
        snow_nulls = snow_cur.fetchone()[0]

        null_summary[col] = {
            "duckdb_nulls": duck_nulls,
            "snowflake_nulls": snow_nulls,
            "match": duck_nulls == snow_nulls,
        }

    row_level_mismatches = 0
    for drow, srow in zip(duck_rows, snow_rows):
        dnorm = normalize_row(drow)
        snorm = normalize_row(srow)

        row_has_diff = False
        for j, col in enumerate(cols):
            if not values_equal(dnorm[j], snorm[j], col):
                row_has_diff = True
                break

        if row_has_diff:
            row_level_mismatches += 1

    result = {
        "logical_name": logical_name,
        "duckdb_table": duck_table,
        "snowflake_table": snow_table,
        "duckdb_rows": duck_count,
        "snowflake_rows": snow_count,
        "row_diff": snow_count - duck_count,
        "raw_checksum_match": raw_checksum_match,
        "row_level_mismatches_after_normalization": row_level_mismatches,
        "null_summary": null_summary,
        "status": (
            "MATCH"
            if duck_count == snow_count and row_level_mismatches == 0
            else "MISMATCH"
        ),
    }
    return result


def main():
    start_time = utc_now_iso()

    duck_con = duckdb.connect(str(DUCKDB_PATH))

    snow_con = snowflake.connector.connect(
        account="LMGBBPF-LUC85498",
        user="ISMAILGARGOURI",
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        authenticator="username_password_mfa",
        warehouse="COMPUTE_WH",
        database="PROD",
        schema="DUCKDB_MIGRATED",
        role="ACCOUNTADMIN",
    )
    snow_cur = snow_con.cursor()

    results = []
    try:
        for logical_name, cfg in TABLE_CONFIG.items():
            print(f"\nComparing {logical_name} ...")
            result = compare_table(duck_con, snow_cur, logical_name, cfg)
            results.append(result)
            print(json.dumps(result, indent=2))
    finally:
        snow_cur.close()
        snow_con.close()
        duck_con.close()

    out_path = ROOT / "experiments" / "duckdb_vs_migrated_snowflake_summary.json"
    out_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\nSaved comparison summary to {out_path}")

    match_count = sum(1 for r in results if r.get("status") == "MATCH")
    append_stage_result(
        stage_name="validate_duckdb_vs_migrated_snowflake",
        start_time=start_time,
        end_time=utc_now_iso(),
        status="success" if match_count == len(results) else "partial_success",
        metadata={
            "tables_compared": len(results),
            "match_count": match_count,
            "mismatch_count": len(results) - match_count,
            "summary_file": str(out_path),
        },
    )


if __name__ == "__main__":
    main()