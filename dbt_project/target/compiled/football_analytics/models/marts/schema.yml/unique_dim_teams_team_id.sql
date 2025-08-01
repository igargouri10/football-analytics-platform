
    
    

select
    team_id as unique_field,
    count(*) as n_records

from "dbt"."main"."dim_teams"
where team_id is not null
group by team_id
having count(*) > 1


