-- models/staging/stg_matches.sql - SNOWFLAKE VERSION
{{ config(materialized='table') }}

WITH source AS (
    SELECT
        *
    FROM
        -- Reference the new raw table in Snowflake
        {{ source('football_data_raw', 'RAW_MATCHES') }}
)
SELECT
    -- The VALUE column from FLATTEN contains each individual match object
    VALUE AS match_data
FROM
    source,
    -- Use Snowflake's FLATTEN function to expand the 'events' array in the raw JSON data
    LATERAL FLATTEN(INPUT => RAW_DATA:events)