{{ config(materialized='table') }}

{% if target.type == 'duckdb' %}

WITH raw AS (
    SELECT *
    FROM read_json_auto(
        's3://{{ env_var("AWS_S3_BUCKET_NAME") }}/raw/thesportsdb/epl_season_2023-2024.json'
    )
)
SELECT
    unnest(events) AS match_data
FROM raw

{% else %}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('football_data_raw', 'RAW_MATCHES') }}
)
SELECT
    f.value AS match_data
FROM
    source,
    LATERAL FLATTEN(input => source.RAW_DATA['events']) f

{% endif %}