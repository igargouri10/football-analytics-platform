import json
import os
from pathlib import Path
import pandas as pd

import duckdb
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from experiments.runtime_logger import append_stage_result, utc_now_iso

DUCKDB_PATH = ROOT / "dbt_project" / "target" / "dbt.duckdb"

SNOWFLAKE_DATABASE = "PROD"
SNOWFLAKE_SCHEMA = "DUCKDB_MIGRATED"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_ROLE = "ACCOUNTADMIN"

TABLE_CONFIG = {
    "dim_teams": {
        "duckdb_query": """
            SELECT
                team_id,
                team_name
            FROM main.dim_teams
            ORDER BY team_id
        """,
        "snowflake_table": "DIM_TEAMS",
        "create_sql": f"""
            CREATE OR REPLACE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.DIM_TEAMS (
                TEAM_ID INTEGER,
                TEAM_NAME STRING
            )
        """,
    },
    "fct_matches": {
        "duckdb_query": """
            SELECT
                match_id,
                league_id,
                season,
                match_date,
                match_status,
                home_team_id,
                home_team_name,
                home_team_score,
                away_team_id,
                away_team_name,
                away_team_score
            FROM main.fct_matches
            ORDER BY match_id
        """,
        "snowflake_table": "FCT_MATCHES",
        "create_sql": f"""
            CREATE OR REPLACE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.FCT_MATCHES (
                MATCH_ID INTEGER,
                LEAGUE_ID INTEGER,
                SEASON STRING,
                MATCH_DATE DATE,
                MATCH_STATUS STRING,
                HOME_TEAM_ID INTEGER,
                HOME_TEAM_NAME STRING,
                HOME_TEAM_SCORE INTEGER,
                AWAY_TEAM_ID INTEGER,
                AWAY_TEAM_NAME STRING,
                AWAY_TEAM_SCORE INTEGER
            )
        """,
    },
    "fct_training_dataset": {
        "duckdb_query": """
            SELECT
                match_id,
                match_date,
                match_result,
                home_team_id,
                home_team_name,
                home_avg_goals_scored_last_5,
                home_avg_goals_conceded_last_5,
                away_team_id,
                away_team_name,
                away_avg_goals_scored_last_5,
                away_avg_goals_conceded_last_5
            FROM main.fct_training_dataset
            ORDER BY match_id
        """,
        "snowflake_table": "FCT_TRAINING_DATASET",
        "create_sql": f"""
            CREATE OR REPLACE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.FCT_TRAINING_DATASET (
                MATCH_ID INTEGER,
                MATCH_DATE DATE,
                MATCH_RESULT STRING,
                HOME_TEAM_ID INTEGER,
                HOME_TEAM_NAME STRING,
                HOME_AVG_GOALS_SCORED_LAST_5 FLOAT,
                HOME_AVG_GOALS_CONCEDED_LAST_5 FLOAT,
                AWAY_TEAM_ID INTEGER,
                AWAY_TEAM_NAME STRING,
                AWAY_AVG_GOALS_SCORED_LAST_5 FLOAT,
                AWAY_AVG_GOALS_CONCEDED_LAST_5 FLOAT
            )
        """,
    },
}

def normalize_dataframe_for_snowflake(df, logical_name: str):
    df = uppercase_columns(df)

    # Convert DATE-like columns to ISO strings so Snowflake can load them safely
    if "MATCH_DATE" in df.columns:
        df["MATCH_DATE"] = pd.to_datetime(df["MATCH_DATE"], errors="coerce").dt.strftime("%Y-%m-%d")

    return df

def get_duckdb_connection():
    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(f"DuckDB file not found: {DUCKDB_PATH}")
    return duckdb.connect(str(DUCKDB_PATH))


def get_snowflake_connection():
    password = os.getenv("SNOWFLAKE_PASSWORD")
    if not password:
        raise EnvironmentError("SNOWFLAKE_PASSWORD is not set in the current shell.")

    return snowflake.connector.connect(
        account="LMGBBPF-LUC85498",
        user="ISMAILGARGOURI",
        password=password,
        authenticator="username_password_mfa",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )


def ensure_schema(cur):
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")


def uppercase_columns(df):
    df.columns = [str(c).upper() for c in df.columns]
    return df


def migrate_table(duck_con, snow_con, snow_cur, logical_name, cfg):
    df = duck_con.execute(cfg["duckdb_query"]).df()
    df = normalize_dataframe_for_snowflake(df, logical_name)

    snow_cur.execute(cfg["create_sql"])

    print(df.dtypes)
    print(df.head())
    success, nchunks, nrows, output = write_pandas(
        snow_con,
        df,
        cfg["snowflake_table"],
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        chunk_size=10000,
        compression="snappy",
    )

    duck_count = len(df)
    snow_cur.execute(
        f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{cfg['snowflake_table']}"
    )
    snow_count = snow_cur.fetchone()[0]

    return {
        "logical_name": logical_name,
        "snowflake_table": f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{cfg['snowflake_table']}",
        "write_success": bool(success),
        "duckdb_rows": duck_count,
        "snowflake_rows": snow_count,
        "chunks": int(nchunks),
        "loaded_rows_reported": int(nrows),
        "copy_output": output,
    }


def main():
    start_time = utc_now_iso()

    duck_con = get_duckdb_connection()
    snow_con = get_snowflake_connection()
    snow_cur = snow_con.cursor()

    results = []
    try:
        ensure_schema(snow_cur)

        for logical_name, cfg in TABLE_CONFIG.items():
            print(f"\nMigrating {logical_name} ...")
            result = migrate_table(duck_con, snow_con, snow_cur, logical_name, cfg)
            results.append(result)
            print(json.dumps(result, indent=2, default=str))

    finally:
        snow_cur.close()
        snow_con.close()
        duck_con.close()

    out_path = ROOT / "experiments" / "duckdb_to_snowflake_migration_summary.json"
    out_path.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    print(f"\nSaved migration summary to {out_path}")

    success_count = sum(1 for r in results if r.get("write_success"))
    append_stage_result(
        stage_name="migrate_duckdb_to_snowflake",
        start_time=start_time,
        end_time=utc_now_iso(),
        status="success" if success_count == len(results) else "partial_success",
        metadata={
            "tables_attempted": len(results),
            "tables_loaded_successfully": success_count,
            "summary_file": str(out_path),
            "row_counts": {
                r["logical_name"]: {
                    "duckdb_rows": r["duckdb_rows"],
                    "snowflake_rows": r["snowflake_rows"],
                }
                for r in results
            },
        },
    )


if __name__ == "__main__":
    main()