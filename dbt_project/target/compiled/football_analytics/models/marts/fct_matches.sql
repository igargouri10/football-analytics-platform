

WITH stg_matches AS (
    SELECT * FROM "PROD"."RAW"."stg_matches"
)
SELECT
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
FROM stg_matches
WHERE (match_data:strStatus)::STRING = 'Match Finished'



  -- this filter will only be applied on an incremental run
  AND match_date > (SELECT max(match_date) FROM "PROD"."RAW"."fct_matches")

