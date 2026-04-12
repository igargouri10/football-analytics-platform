
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    match_id as unique_field,
    count(*) as n_records

from "batch_C_mixed_full_stack__manual_plus_llm"."main"."fct_matches"
where match_id is not null
group by match_id
having count(*) > 1



  
  
      
    ) dbt_internal_test