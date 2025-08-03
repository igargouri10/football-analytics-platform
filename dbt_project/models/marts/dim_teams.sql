{{ config(materialized='table') }}

WITH stg_matches AS (
    SELECT * FROM {{ ref('stg_matches') }}
)
, home_teams AS (
    SELECT
        (match_data:idHomeTeam)::INT AS team_id,
        (match_data:strHomeTeam)::STRING AS team_name
    FROM stg_matches
)
, away_teams AS (
    SELECT
        (match_data:idAwayTeam)::INT AS team_id,
        (match_data:strAwayTeam)::STRING AS team_name
    FROM stg_matches
)
, all_teams AS (
    SELECT * FROM home_teams
    UNION ALL
    SELECT * FROM away_teams
)
SELECT DISTINCT
    team_id,
    team_name
FROM all_teams
ORDER BY team_name