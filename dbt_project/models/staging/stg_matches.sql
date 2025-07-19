-- models/staging/stg_matches.sql

with source as (

    select * from read_json_auto('s3://{{ env_var("AWS_S3_BUCKET_NAME") }}/raw/matches/*.json')

),

renamed as (

    select
        unnest(matches) as match_data
    from source

),

final as (

    select
        (match_data ->> 'id')::int as match_id,
        (match_data ->> 'utcDate')::timestamp as match_date,
        match_data ->> 'status' as match_status,

        (match_data -> 'homeTeam' ->> 'id')::int as home_team_id,
        (match_data -> 'homeTeam' ->> 'name') as home_team_name,
        (match_data -> 'score' -> 'fullTime' ->> 'home')::int as home_team_score,

        (match_data -> 'awayTeam' ->> 'id')::int as away_team_id,
        (match_data -> 'awayTeam' ->> 'name') as away_team_name,
        (match_data -> 'score' -> 'fullTime' ->> 'away')::int as away_team_score,

        (match_data -> 'competition' ->> 'name') as competition_name,
        (match_data -> 'season' ->> 'currentMatchday')::int as matchday,

        -- This window function assigns a row number to each match_id,
        -- ordering by the most recent date first.
        row_number() over (partition by (match_data ->> 'id')::int order by (match_data ->> 'utcDate')::timestamp desc) as row_num

    from renamed
)

-- Finally, select only the most recent record for each match to remove duplicates.
select * from final where row_num = 1