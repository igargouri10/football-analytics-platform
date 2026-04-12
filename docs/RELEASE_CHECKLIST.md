# GitHub Release Checklist

## Before pushing
- [ ] `.env` is not tracked
- [ ] `.dbt/profiles.yml` with real credentials is not tracked
- [ ] `.venv/` is ignored
- [ ] local `.duckdb` files are ignored
- [ ] Airflow runtime junk is ignored
- [ ] README.md is updated
- [ ] `.env.example` is updated
- [ ] setup docs are updated
- [ ] experiment commands in README were tested
- [ ] key result artifacts are present if you want a reproducibility snapshot

## Recommended first public push
```bash
git add README.md .gitignore .env.example .dbt/profiles.example.yml docs/ requirements.txt airflow/ dbt_project/ llm_tests/ experiments/
git commit -m "Document project setup and reproducibility workflow"
git push origin main
```
