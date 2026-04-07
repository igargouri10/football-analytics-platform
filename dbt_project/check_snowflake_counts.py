import os
import snowflake.connector

conn = snowflake.connector.connect(
    account="LMGBBPF-LUC85498",
    user="ISMAILGARGOURI",
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    authenticator="username_password_mfa",
    warehouse="COMPUTE_WH",
    database="PROD",
    schema="RAW",
    role="ACCOUNTADMIN",
)

queries = [
    ("stg_matches_rows", 'SELECT COUNT(*) FROM PROD.RAW."stg_matches"'),
    ("dim_teams_rows", 'SELECT COUNT(*) FROM PROD.RAW."dim_teams"'),
    ("fct_matches_rows", 'SELECT COUNT(*) FROM PROD.RAW."fct_matches"'),
    ("fct_training_dataset_rows", 'SELECT COUNT(*) FROM PROD.RAW."fct_training_dataset"'),
]

cur = conn.cursor()
try:
    for label, q in queries:
        cur.execute(q)
        print(label, cur.fetchone()[0])

    for table in ["stg_matches", "dim_teams", "fct_matches", "fct_training_dataset"]:
        print(f"\nPreview: {table}")
        cur.execute(f'SELECT * FROM PROD.RAW."{table}" LIMIT 5')
        for row in cur.fetchall():
            print(row)
finally:
    cur.close()
    conn.close()