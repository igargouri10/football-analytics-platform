import copy
from pathlib import Path
import yaml

BASE_SCHEMA = Path("dbt_project/models/marts/schema.yml")
GENERATED_DIR = Path("dbt_project/generated_tests")
BACKUP_SCHEMA = Path("dbt_project/models/marts/schema.manual_backup.yml.txt")


def load_yaml(path: Path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def dump_yaml(path: Path, data):
    path.write_text(
        yaml.safe_dump(data, sort_keys=False, allow_unicode=True),
        encoding="utf-8"
    )


def normalize_test(test_item):
    if isinstance(test_item, str):
        return test_item
    return yaml.safe_dump(test_item, sort_keys=True)


def merge_column_tests(base_col: dict, gen_col: dict):
    base_tests = base_col.get("tests", []) or []
    gen_tests = gen_col.get("tests", []) or []

    seen = {normalize_test(t) for t in base_tests}
    merged = list(base_tests)

    for t in gen_tests:
        key = normalize_test(t)
        if key not in seen:
            merged.append(t)
            seen.add(key)

    if merged:
        base_col["tests"] = merged
    return base_col


def merge_model(base_model: dict, gen_model: dict):
    base_columns = {col["name"]: col for col in base_model.get("columns", [])}
    gen_columns = {col["name"]: col for col in gen_model.get("columns", [])}

    for col_name, gen_col in gen_columns.items():
        if col_name in base_columns:
            base_columns[col_name] = merge_column_tests(base_columns[col_name], gen_col)
        else:
            base_columns[col_name] = copy.deepcopy(gen_col)

    ordered_cols = []
    already = set()

    for col in base_model.get("columns", []):
        name = col["name"]
        ordered_cols.append(base_columns[name])
        already.add(name)

    for col_name, col in base_columns.items():
        if col_name not in already:
            ordered_cols.append(col)

    base_model["columns"] = ordered_cols
    return base_model


def main():
    base = load_yaml(BASE_SCHEMA)

    if not BACKUP_SCHEMA.exists():
        BACKUP_SCHEMA.write_text(BASE_SCHEMA.read_text(encoding="utf-8"), encoding="utf-8")

    base_models = {m["name"]: m for m in base.get("models", [])}

    for gen_file in sorted(GENERATED_DIR.glob("*_llm_tests.yml")):
        gen = load_yaml(gen_file)
        gen_models = gen.get("models", []) or []

        for gen_model in gen_models:
            model_name = gen_model["name"]
            if model_name in base_models:
                base_models[model_name] = merge_model(base_models[model_name], gen_model)
            else:
                base_models[model_name] = copy.deepcopy(gen_model)

    base["models"] = list(base_models.values())
    dump_yaml(BASE_SCHEMA, base)

    print(f"Merged LLM tests into {BASE_SCHEMA}")
    print(f"Backup saved at {BACKUP_SCHEMA}")


if __name__ == "__main__":
    main()