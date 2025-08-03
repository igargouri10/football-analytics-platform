-- models/staging/stg_matches.sql - SNOWFLAKE VERSION (REVISED)
{{ config(materialized='table') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('football_data_raw', 'RAW_MATCHES') }}
)
SELECT
    -- Use an alias 'f' for the flatten function
    -- Access the 'events' key using bracket notation for robustness
    f.value AS match_data
FROM
    source,
    LATERAL FLATTEN(input => source.RAW_DATA['events']) f