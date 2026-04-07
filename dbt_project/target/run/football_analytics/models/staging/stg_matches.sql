
  
    

        create or replace transient table "PROD"."RAW"."stg_matches"
         as
        (



WITH source AS (
    SELECT
        *
    FROM
        PROD.RAW.RAW_MATCHES
)
SELECT
    f.value AS match_data
FROM
    source,
    LATERAL FLATTEN(input => source.RAW_DATA['events']) f


        );
      
  