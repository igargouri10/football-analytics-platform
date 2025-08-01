select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select team_id
from "dbt"."main"."dim_teams"
where team_id is null



      
    ) dbt_internal_test