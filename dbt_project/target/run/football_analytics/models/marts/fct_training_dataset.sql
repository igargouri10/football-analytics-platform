
  create or replace   view "PROD"."RAW"."fct_training_dataset"
  
   as (
    -- models/marts/fct_training_dataset.sql

WITH matches AS (
    SELECT
        match_id,
        match_date,
        home_team_id,
        home_team_name,
        away_team_id,
        away_team_name,
        home_team_score,
        away_team_score,
        CASE
            WHEN home_team_score > away_team_score THEN 'HOME_WIN'
            WHEN away_team_score > home_team_score THEN 'AWAY_WIN'
            ELSE 'DRAW'
        END AS match_result
    FROM "PROD"."RAW"."fct_matches"
),

-- Unpivot the data to have one row per team per match
team_matches AS (
    SELECT
        match_id,
        match_date,
        home_team_id AS team_id,
        home_team_score AS goals_scored,
        away_team_score AS goals_conceded
    FROM matches

    UNION ALL

    SELECT
        match_id,
        match_date,
        away_team_id AS team_id,
        away_team_score AS goals_scored,
        home_team_score AS goals_conceded
    FROM matches
),

-- Calculate rolling features using window functions
team_rolling_features AS (
    SELECT
        team_id,
        match_date,
        -- Calculate the average goals scored over the last 5 games for a team, excluding the current game
        AVG(goals_scored) OVER (
            PARTITION BY team_id
            ORDER BY match_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_goals_scored_last_5,

        -- Calculate the average goals conceded over the last 5 games for a team, excluding the current game
        AVG(goals_conceded) OVER (
            PARTITION BY team_id
            ORDER BY match_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_goals_conceded_last_5
    FROM
        team_matches
),

-- Aggregate to get the latest features for each team before a given match
team_features AS (
    SELECT
        team_id,
        match_date,
        -- Use MAX to get the single feature value for that date, as the window function calculates it for every match
        MAX(avg_goals_scored_last_5) AS avg_goals_scored_last_5,
        MAX(avg_goals_conceded_last_5) AS avg_goals_conceded_last_5
    FROM team_rolling_features
    GROUP BY 1, 2
),

-- Join the features back to the original matches table
final_dataset AS (
    SELECT
        m.match_id,
        m.match_date,
        m.match_result,

        -- Home team features
        m.home_team_id,
        m.home_team_name,
        COALESCE(home_features.avg_goals_scored_last_5, 0) AS home_avg_goals_scored_last_5,
        COALESCE(home_features.avg_goals_conceded_last_5, 0) AS home_avg_goals_conceded_last_5,

        -- Away team features
        m.away_team_id,
        m.away_team_name,
        COALESCE(away_features.avg_goals_scored_last_5, 0) AS away_avg_goals_scored_last_5,
        COALESCE(away_features.avg_goals_conceded_last_5, 0) AS away_avg_goals_conceded_last_5

    FROM
        matches AS m
    LEFT JOIN
        team_features AS home_features ON m.home_team_id = home_features.team_id AND m.match_date = home_features.match_date
    LEFT JOIN
        team_features AS away_features ON m.away_team_id = away_features.team_id AND m.match_date = away_features.match_date
)

SELECT * FROM final_dataset
  );

