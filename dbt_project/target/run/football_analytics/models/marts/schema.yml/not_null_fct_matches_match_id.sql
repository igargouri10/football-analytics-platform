
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select match_id
from "PROD"."RAW"."fct_matches"
where match_id is null



  
  
      
    ) dbt_internal_test