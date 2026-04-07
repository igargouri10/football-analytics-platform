import duckdb

con = duckdb.connect(r"target/dbt.duckdb")

queries = [
    ("stg_matches_rows", "SELECT COUNT(*) FROM main.stg_matches"),
    ("dim_teams_rows", "SELECT COUNT(*) FROM main.dim_teams"),
    ("fct_matches_rows", "SELECT COUNT(*) FROM main.fct_matches"),
    ("fct_training_dataset_rows", "SELECT COUNT(*) FROM main.fct_training_dataset"),
]

for label, q in queries:
    print(label, con.execute(q).fetchone()[0])

for table in ["stg_matches", "dim_teams", "fct_matches", "fct_training_dataset"]:
    print(f"\nPreview: {table}")
    rows = con.execute(f"SELECT * FROM main.{table} LIMIT 5").fetchall()
    for row in rows:
        print(row)

con.close()
