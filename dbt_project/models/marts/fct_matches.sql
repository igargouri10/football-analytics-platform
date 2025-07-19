-- models/marts/fct_matches.sql

select
    -- Identifiers
    match_id,
    match_date,
    home_team_id,
    away_team_id,

    -- Match results
    home_team_score,
    away_team_score,
    
    -- Calculated metric
    (home_team_score - away_team_score) as home_team_goal_difference

from {{ ref('stg_matches') }}

-- We only want to include finished matches for our analysis
-- where match_status = 'FINISHED'