# dashboard/app.py - FINAL, GUARANTEED VERSION

import streamlit as st
import pandas as pd
import duckdb
import os

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Football Analytics Dashboard",
    page_icon="⚽",
    layout="wide",
)

# --- DATA LOADING ---
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'dbt_project', 'target', 'dbt.duckdb')

@st.cache_data
def load_data():
    """Connects to DuckDB, loads the fact and dimension tables, and returns them as pandas DataFrames."""
    con = duckdb.connect(DB_PATH, read_only=True)
    
    # FINAL, ROBUST SQL QUERY USING CTES
    matches_df = con.execute("""
        WITH matches AS (
            SELECT * FROM fct_matches
        ),
        home_teams AS (
            SELECT team_id, team_name AS home_team_name FROM dim_teams
        ),
        away_teams AS (
            SELECT team_id, team_name AS away_team_name FROM dim_teams
        )
        SELECT
            matches.match_id,
            matches.match_date,
            home_teams.home_team_name,
            away_teams.away_team_name,
            matches.home_team_score,
            matches.away_team_score
        FROM matches
        LEFT JOIN home_teams ON matches.home_team_id = home_teams.team_id
        LEFT JOIN away_teams ON matches.away_team_id = away_teams.team_id
        ORDER BY matches.match_date DESC
    """).df()

    # Load dimension table for the dropdown
    teams_df = con.execute("SELECT team_name FROM dim_teams ORDER BY team_name ASC").df()
    
    con.close()
    
    matches_df['match_date'] = pd.to_datetime(matches_df['match_date'])
    
    return matches_df, teams_df

# --- MAIN APPLICATION LOGIC ---
try:
    matches_df, teams_df = load_data()

    # --- UI LAYOUT ---
    st.title("⚽ Football Analytics Dashboard")
    st.markdown("An interactive dashboard to explore Premier League match results, powered by a full dbt and Airflow pipeline.")

    # --- SIDEBAR FOR TEAM SELECTION ---
    st.sidebar.header("Filter by Team")
    team_list = ["All Teams"] + sorted(teams_df['team_name'].unique().tolist())
    selected_team = st.sidebar.selectbox("Select a Team", team_list)

    # --- FILTER DATA BASED ON SELECTION ---
    if selected_team == "All Teams":
        filtered_matches = matches_df
    else:
        filtered_matches = matches_df[
            (matches_df['home_team_name'] == selected_team) | 
            (matches_df['away_team_name'] == selected_team)
        ]

    # --- MAIN PAGE DISPLAY ---
    st.header(f"Displaying Results for: {selected_team}")

    if selected_team != "All Teams":
        # Filter out unplayed matches BEFORE calculating stats
        played_matches = filtered_matches.dropna(subset=['home_team_score', 'away_team_score'])
        
        wins, losses, draws = 0, 0, 0
        # Iterate over PLAYED matches only
        for _, row in played_matches.iterrows():
            is_home_team = (row['home_team_name'] == selected_team)
            if row['home_team_score'] == row['away_team_score']:
                draws += 1
            elif (is_home_team and row['home_team_score'] > row['away_team_score']) or \
                 (not is_home_team and row['away_team_score'] > row['home_team_score']):
                wins += 1
            else:
                losses += 1
        col1, col2, col3 = st.columns(3)
        col1.metric("Wins", wins)
        col2.metric("Losses", losses)
        col3.metric("Draws", draws)

    st.subheader("Cumulative Goal Difference Over Time")
    if not filtered_matches.empty and selected_team != "All Teams":
        # Filter out unplayed matches for the chart
        chart_data = filtered_matches.dropna(subset=['home_team_score', 'away_team_score']).copy().sort_values(by='match_date')
        
        if not chart_data.empty:
            chart_data['goal_difference'] = chart_data.apply(
                lambda row: row['home_team_score'] - row['away_team_score'] if row['home_team_name'] == selected_team else row['away_team_score'] - row['home_team_score'],
                axis=1
            )
            chart_data['cumulative_goal_difference'] = chart_data['goal_difference'].cumsum()
            chart_data = chart_data.set_index('match_date')
            st.line_chart(chart_data['cumulative_goal_difference'])
        else:
            st.info("No played matches with scores available to plot a trend.")
    else:
        st.info("Select a specific team to view their goal difference trend.")

    st.subheader("Match Results")
    st.dataframe(
        filtered_matches,
        column_config={ "match_id": None, "match_date": "Date", "home_team_name": "Home Team", "away_team_name": "Away Team", "home_team_score": "Home Score", "away_team_score": "Away Score", },
        hide_index=True,
    )

except Exception as e:
    st.error(f"An error occurred while loading the data: {e}")
    st.info("Please ensure your dbt models have been run successfully (`dbt run`). The data warehouse file (`dbt_project/target/dbt.duckdb`) may be missing or empty.")