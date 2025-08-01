-- models/staging/stg_matches.sql
{{ config(materialized='table') }}

WITH source AS (
    -- Reference the raw data source we defined in sources.yml
    SELECT * FROM {{ source('football_data_raw', 'matches') }}
)

SELECT
    -- Unnest the nested 'events' array to get one row per match
    UNNEST(events) as match_data
FROM
    source