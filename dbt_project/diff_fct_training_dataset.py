import os
from decimal import Decimal
from datetime import date, datetime
import duckdb
import snowflake.connector

NUMERIC_COLS = {
    "home_avg_goals_scored_last_5",
    "home_avg_goals_conceded_last_5",
    "away_avg_goals_scored_last_5",
    "away_avg_goals_conceded_last_5",
}

COLS = [
    "match_id", "match_date", "match_result",
    "home_team_id", "home_team_name",
    "home_avg_goals_scored_last_5", "home_avg_goals_conceded_last_5",
    "away_team_id", "away_team_name",
    "away_avg_goals_scored_last_5", "away_avg_goals_conceded_last_5",
]

def norm(v, col=None):
    if v is None:
        return None
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, Decimal):
        v = float(v)
    if col in NUMERIC_COLS:
        return float(v)
    return v

def values_equal(a, b, col):
    if col in NUMERIC_COLS:
        if a is None and b is None:
            return True
        if a is None or b is None:
            return False
        return abs(float(a) - float(b)) <= 0.0011
    return a == b

duck = duckdb.connect(r"target/dbt.duckdb")
snow = snowflake.connector.connect(
    account="LMGBBPF-LUC85498",
    user="ISMAILGARGOURI",
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    authenticator="username_password_mfa",
    warehouse="COMPUTE_WH",
    database="PROD",
    schema="RAW",
    role="ACCOUNTADMIN",
)
cur = snow.cursor()

try:
    duck_rows = duck.execute("""
        SELECT
            match_id, match_date, match_result,
            home_team_id, home_team_name,
            home_avg_goals_scored_last_5, home_avg_goals_conceded_last_5,
            away_team_id, away_team_name,
            away_avg_goals_scored_last_5, away_avg_goals_conceded_last_5
        FROM main.fct_training_dataset
        ORDER BY match_id
    """).fetchall()

    cur.execute("""
        SELECT
            match_id, match_date, match_result,
            home_team_id, home_team_name,
            home_avg_goals_scored_last_5, home_avg_goals_conceded_last_5,
            away_team_id, away_team_name,
            away_avg_goals_scored_last_5, away_avg_goals_conceded_last_5
        FROM PROD.RAW."fct_training_dataset"
        ORDER BY match_id
    """)
    snow_rows = cur.fetchall()

    print("DuckDB rows:", len(duck_rows))
    print("Snowflake rows:", len(snow_rows))

    if len(duck_rows) != len(snow_rows):
        print("\nRow count mismatch detected. Cannot safely do full row-by-row comparison.")

    mismatches = 0
    for i, (drow, srow) in enumerate(zip(duck_rows, snow_rows), start=1):
        dnorm = [norm(v, COLS[j]) for j, v in enumerate(drow)]
        snorm = [norm(v, COLS[j]) for j, v in enumerate(srow)]

        row_has_diff = any(
            not values_equal(dnorm[j], snorm[j], col)
            for j, col in enumerate(COLS)
        )

        if row_has_diff:
            mismatches += 1
            print(f"\nMismatch at row {i}, match_id={dnorm[0]}")
            for j, col in enumerate(COLS):
                if not values_equal(dnorm[j], snorm[j], col):
                    print(f"  {col}: DuckDB={dnorm[j]!r} | Snowflake={snorm[j]!r}")
            if mismatches >= 10:
                break

    if mismatches == 0:
        print("\nNo mismatches after normalization.")
    else:
        print(f"\nTotal mismatched rows found (stopped early if >10): {mismatches}")

finally:
    cur.close()
    snow.close()
    duck.close()