-- models/marts/dim_teams.sql

with all_teams as (

    -- Select home teams from the staging model
    select
        home_team_id as team_id,
        home_team_name as team_name
    from {{ ref('stg_matches') }}

    union all

    -- Select away teams from the staging model
    select
        away_team_id as team_id,
        away_team_name as team_name
    from {{ ref('stg_matches') }}

)

-- The UNION ALL operation might create duplicates. We use GROUP BY
-- to get a unique list of teams.
select
    team_id,
    team_name
from all_teams
group by 1, 2
order by team_id