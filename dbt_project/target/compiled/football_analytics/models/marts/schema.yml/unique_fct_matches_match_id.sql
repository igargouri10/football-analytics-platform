
    
    

select
    match_id as unique_field,
    count(*) as n_records

from "PROD"."RAW"."fct_matches"
where match_id is not null
group by match_id
having count(*) > 1


