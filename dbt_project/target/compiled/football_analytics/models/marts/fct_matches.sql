-- models/marts/fct_matches.sql - FINAL VERSION


WITH stg_matches AS (
    SELECT * FROM "dbt"."main"."stg_matches"
),

transformed AS (
    SELECT
        -- Match details
        (match_data ->> 'idEvent')::INT AS match_id,
        (match_data ->> 'idLeague')::INT AS league_id,
        (match_data ->> 'strSeason') AS season,
        (match_data ->> 'dateEvent')::DATE AS match_date,
        (match_data ->> 'strStatus') AS match_status,

        -- Home team details
        (match_data ->> 'idHomeTeam')::INT AS home_team_id,
        (match_data ->> 'strHomeTeam') AS home_team_name,
        (match_data ->> 'intHomeScore')::INT AS home_team_score,

        -- Away team details
        (match_data ->> 'idAwayTeam')::INT AS away_team_id,
        (match_data ->> 'strAwayTeam') AS away_team_name,
        (match_data ->> 'intAwayScore')::INT AS away_team_score,

        -- Add a row number to handle potential duplicates from the source API
        ROW_NUMBER() OVER(PARTITION BY (match_data ->> 'idEvent')::INT ORDER BY (match_data ->> 'dateEvent')::DATE DESC) as row_num

    FROM
        stg_matches
    WHERE
        -- Only include matches that have actually finished
        (match_data ->> 'strStatus') = 'Match Finished'
)

-- Select only the most recent record for each match to ensure uniqueness
SELECT *
FROM transformed
WHERE row_num = 1