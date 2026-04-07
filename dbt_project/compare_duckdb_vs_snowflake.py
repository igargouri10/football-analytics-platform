import os
import json
import hashlib
import duckdb
import snowflake.connector
from datetime import date, datetime, time
from decimal import Decimal

DUCKDB_PATH = r"target/dbt.duckdb"

TABLE_CONFIG = {
    "stg_matches": {
        "duckdb_table": "main.stg_matches",
        "snowflake_table": 'PROD.RAW."stg_matches"',
        "columns": ["match_data"],
        "duckdb_order_by": "CAST(match_data.idEvent AS INTEGER)",
        "snowflake_order_by": "TRY_TO_NUMBER(TO_VARCHAR(match_data:idEvent))",
    },
    "dim_teams": {
        "duckdb_table": "main.dim_teams",
        "snowflake_table": 'PROD.RAW."dim_teams"',
        "columns": ["team_id", "team_name"],
        "duckdb_order_by": "team_id",
        "snowflake_order_by": "team_id",
    },
    "fct_matches": {
        "duckdb_table": "main.fct_matches",
        "snowflake_table": 'PROD.RAW."fct_matches"',
        "columns": [
            "match_id", "league_id", "season", "match_date", "match_status",
            "home_team_id", "home_team_name", "home_team_score",
            "away_team_id", "away_team_name", "away_team_score",
        ],
        "duckdb_order_by": "match_id",
        "snowflake_order_by": "match_id",
    },
    "fct_training_dataset": {
        "duckdb_table": "main.fct_training_dataset",
        "snowflake_table": 'PROD.RAW."fct_training_dataset"',
        "columns": [
            "match_id", "match_date", "match_result",
            "home_team_id", "home_team_name",
            "home_avg_goals_scored_last_5", "home_avg_goals_conceded_last_5",
            "away_team_id", "away_team_name",
            "away_avg_goals_scored_last_5", "away_avg_goals_conceded_last_5",
        ],
        "duckdb_order_by": "match_id",
        "snowflake_order_by": "match_id",
    },
}


def normalize_value(v):
    if v is None:
        return None

    if isinstance(v, (date, datetime, time)):
        return v.isoformat()

    if isinstance(v, Decimal):
        return float(v)

    if isinstance(v, dict):
        return {str(k): normalize_value(val) for k, val in sorted(v.items(), key=lambda x: str(x[0]))}

    if isinstance(v, (list, tuple)):
        return [normalize_value(x) for x in v]

    if isinstance(v, str):
        s = v.strip()
        if s.startswith("{") and s.endswith("}"):
            try:
                return normalize_value(json.loads(s))
            except Exception:
                return v
        if s.startswith("[") and s.endswith("]"):
            try:
                return normalize_value(json.loads(s))
            except Exception:
                return v

    return v


def normalize_row(row):
    return [normalize_value(v) for v in row]


def checksum_rows(rows):
    normalized = [normalize_row(r) for r in rows]
    payload = json.dumps(normalized, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def duckdb_fetch(con, query):
    return con.execute(query).fetchall()


def snowflake_fetch(cur, query):
    cur.execute(query)
    return cur.fetchall()


def compare_table(duck_con, snow_cur, name, cfg):
    duck_table = cfg["duckdb_table"]
    snow_table = cfg["snowflake_table"]
    cols = cfg["columns"]
    col_csv = ", ".join(cols)

    duck_count = duck_con.execute(f"SELECT COUNT(*) FROM {duck_table}").fetchone()[0]
    snow_cur.execute(f"SELECT COUNT(*) FROM {snow_table}")
    snow_count = snow_cur.fetchone()[0]

    duck_rows = duckdb_fetch(
        duck_con,
        f"SELECT {col_csv} FROM {duck_table} ORDER BY {cfg['duckdb_order_by']}"
    )
    snow_rows = snowflake_fetch(
        snow_cur,
        f"SELECT {col_csv} FROM {snow_table} ORDER BY {cfg['snowflake_order_by']}"
    )

    duck_checksum = checksum_rows(duck_rows)
    snow_checksum = checksum_rows(snow_rows)

    null_summary = {}
    for col in cols:
        duck_nulls = duck_con.execute(
            f"SELECT COUNT(*) FROM {duck_table} WHERE {col} IS NULL"
        ).fetchone()[0]
        snow_cur.execute(
            f"SELECT COUNT(*) FROM {snow_table} WHERE {col} IS NULL"
        )
        snow_nulls = snow_cur.fetchone()[0]
        null_summary[col] = {
            "duckdb_nulls": duck_nulls,
            "snowflake_nulls": snow_nulls,
            "match": duck_nulls == snow_nulls,
        }

    result = {
        "table": name,
        "duckdb_rows": duck_count,
        "snowflake_rows": snow_count,
        "row_diff": snow_count - duck_count,
        "checksum_match": duck_checksum == snow_checksum,
        "duckdb_checksum": duck_checksum,
        "snowflake_checksum": snow_checksum,
        "status": "MATCH" if (duck_count == snow_count and duck_checksum == snow_checksum) else "MISMATCH",
        "null_summary": null_summary,
    }
    return result


def main():
    duck_con = duckdb.connect(DUCKDB_PATH)

    snow_conn = snowflake.connector.connect(
        account="LMGBBPF-LUC85498",
        user="ISMAILGARGOURI",
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        authenticator="username_password_mfa",
        warehouse="COMPUTE_WH",
        database="PROD",
        schema="RAW",
        role="ACCOUNTADMIN",
    )
    snow_cur = snow_conn.cursor()

    results = []
    try:
        for name, cfg in TABLE_CONFIG.items():
            print(f"\nComparing {name} ...")
            result = compare_table(duck_con, snow_cur, name, cfg)
            results.append(result)
            print(json.dumps(result, indent=2))
    finally:
        snow_cur.close()
        snow_conn.close()
        duck_con.close()

    with open("cross_store_validation_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print("\nSaved results to cross_store_validation_results.json")


if __name__ == "__main__":
    main()