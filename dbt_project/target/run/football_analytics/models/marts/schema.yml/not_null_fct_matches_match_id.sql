
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select match_id
from "batch_C_mixed_full_stack__manual_plus_llm"."main"."fct_matches"
where match_id is null



  
  
      
    ) dbt_internal_test