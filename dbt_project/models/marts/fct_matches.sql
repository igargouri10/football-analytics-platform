{{
  config(
    materialized='incremental',
    unique_key='match_id'
  )
}}

WITH stg_matches AS (
    SELECT * FROM {{ ref('stg_matches') }}
)
SELECT
    {% if target.type == 'duckdb' %}
    CAST(match_data.idEvent AS INTEGER) AS match_id,
    CAST(match_data.idLeague AS INTEGER) AS league_id,
    CAST(match_data.strSeason AS VARCHAR) AS season,
    CAST(match_data.dateEvent AS DATE) AS match_date,
    CAST(match_data.strStatus AS VARCHAR) AS match_status,
    CAST(match_data.idHomeTeam AS INTEGER) AS home_team_id,
    CAST(match_data.strHomeTeam AS VARCHAR) AS home_team_name,
    CAST(match_data.intHomeScore AS INTEGER) AS home_team_score,
    CAST(match_data.idAwayTeam AS INTEGER) AS away_team_id,
    CAST(match_data.strAwayTeam AS VARCHAR) AS away_team_name,
    CAST(match_data.intAwayScore AS INTEGER) AS away_team_score
    {% else %}
    (match_data:idEvent)::INT AS match_id,
    (match_data:idLeague)::INT AS league_id,
    (match_data:strSeason)::STRING AS season,
    (match_data:dateEvent)::DATE AS match_date,
    (match_data:strStatus)::STRING AS match_status,
    (match_data:idHomeTeam)::INT AS home_team_id,
    (match_data:strHomeTeam)::STRING AS home_team_name,
    (match_data:intHomeScore)::INT AS home_team_score,
    (match_data:idAwayTeam)::INT AS away_team_id,
    (match_data:strAwayTeam)::STRING AS away_team_name,
    (match_data:intAwayScore)::INT AS away_team_score
    {% endif %}
FROM stg_matches
WHERE
    {% if target.type == 'duckdb' %}
    CAST(match_data.strStatus AS VARCHAR) = 'Match Finished'
    {% else %}
    (match_data:strStatus)::STRING = 'Match Finished'
    {% endif %}

{% if is_incremental() %}

  AND match_date > (SELECT max(match_date) FROM {{ this }})

{% endif %}