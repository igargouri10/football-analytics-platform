from textwrap import dedent

SYSTEM_PROMPT = dedent("""
You are a senior analytics engineer writing dbt schema tests.

Your task is to generate ONLY valid YAML for dbt tests.
Do not include markdown fences.
Do not include explanations.
Do not invent columns that are not present in the schema.
Prefer realistic tests that are useful and executable.

Allowed test styles:
- unique
- not_null
- accepted_values
- relationships

Use conservative, high-confidence rules only.
If a column should not receive a test, omit it.
""").strip()


def build_user_prompt(model_name: str, columns: list[dict], sample_rows: list[dict]) -> str:
    column_lines = []
    for col in columns:
        column_lines.append(f"- {col['name']} ({col['type']})")

    sample_text = []
    for i, row in enumerate(sample_rows, start=1):
        sample_text.append(f"Row {i}: {row}")

    return dedent(f"""
    Generate a dbt schema YAML definition for model `{model_name}`.

    Schema:
    {chr(10).join(column_lines)}

    Sample rows:
    {chr(10).join(sample_text)}

    Requirements:
    - Output YAML only
    - Top-level keys must be: version, models
    - Include exactly one model entry for `{model_name}`
    - Only use columns that appear in the provided schema
    - Prefer useful tests over many tests
    - Avoid redundant tests
    - Do not create relationship tests unless the foreign-key target is obvious from the schema

    Return format example:

    version: 2
    models:
      - name: example_model
        columns:
          - name: some_column
            tests:
              - not_null
              - unique
    """).strip()