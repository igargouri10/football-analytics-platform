
  
    
    

    create  table
      "dbt"."main"."dim_teams__dbt_tmp"
  
    as (
      -- models/marts/dim_teams.sql


WITH stg_matches AS (
    -- Get the raw JSON data for each match from the staging model
    SELECT * FROM "dbt"."main"."stg_matches"
),

home_teams AS (
    -- Extract the home team id and name from the JSON
    SELECT
        (match_data ->> 'idHomeTeam')::INT AS team_id,
        (match_data ->> 'strHomeTeam') AS team_name
    FROM stg_matches
),

away_teams AS (
    -- Extract the away team id and name from the JSON
    SELECT
        (match_data ->> 'idAwayTeam')::INT AS team_id,
        (match_data ->> 'strAwayTeam') AS team_name
    FROM stg_matches
),

all_teams AS (
    -- Combine the home and away teams into a single list
    SELECT * FROM home_teams
    UNION ALL
    SELECT * FROM away_teams
)

-- Get the unique list of teams
SELECT DISTINCT
    team_id,
    team_name
FROM all_teams
ORDER BY team_name
    );
  
  