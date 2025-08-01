
  
    
    

    create  table
      "dbt"."main"."stg_matches__dbt_tmp"
  
    as (
      -- models/staging/stg_matches.sql


WITH source AS (
    -- Reference the raw data source we defined in sources.yml
    SELECT * FROM 's3://ismailgargouri-football-data-lake-useast1/raw/thesportsdb/epl_season_2023-2024.json'
)

SELECT
    -- Unnest the nested 'events' array to get one row per match
    UNNEST(events) as match_data
FROM
    source
    );
  
  