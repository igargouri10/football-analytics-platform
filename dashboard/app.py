# dashboard/app.py - FINAL VERSION with AI Match Predictor

import streamlit as st
import pandas as pd
import duckdb
import os
import joblib

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Football Analytics Dashboard",
    page_icon="⚽",
    layout="wide",
)

# --- DATA LOADING ---
# Correctly locate the dbt.duckdb file
DB_PATH = os.path.join(os.path.dirname(__file__), 'dbt.duckdb')

@st.cache_data
def load_data():
    """Connects to DuckDB, loads tables, and returns them as pandas DataFrames."""
    con = duckdb.connect(DB_PATH, read_only=True)
    
    matches_df = con.execute("SELECT * FROM fct_matches").df()
    teams_df = con.execute("SELECT team_id, team_name FROM dim_teams ORDER BY team_name ASC").df()
    training_df = con.execute("SELECT * FROM fct_training_dataset").df()
    
    con.close()
    
    matches_df['match_date'] = pd.to_datetime(matches_df['match_date'])
    
    return matches_df, teams_df, training_df

# --- MODEL LOADING ---
@st.cache_resource
def load_model():
    """Loads the trained model from the .pkl file."""
    model = joblib.load(os.path.join(os.path.dirname(__file__), 'match_predictor.pkl'))
    return model

# --- MAIN APPLICATION LOGIC ---
try:
    matches_df, teams_df, training_df = load_data()
    model = load_model()

    # --- UI LAYOUT ---
    st.title("⚽ Football Analytics Dashboard")
    st.markdown("An interactive dashboard to explore match results and predict outcomes.")

    # --- SIDEBAR FOR TEAM SELECTION ---
    st.sidebar.header("Filter by Team")
    team_list = ["All Teams"] + teams_df['team_name'].tolist()
    selected_team = st.sidebar.selectbox("Select a Team", team_list)

    # --- MAIN PAGE ---
    
    # --- AI MATCH PREDICTOR ---
    st.header("🔮 AI Match Predictor")
    
    team_names = teams_df['team_name'].tolist()
    col1, col2 = st.columns(2)
    home_team_selection = col1.selectbox("Select Home Team", team_names, index=0)
    away_team_selection = col2.selectbox("Select Away Team", team_names, index=1)

    if st.button("Predict Outcome"):
        if home_team_selection == away_team_selection:
            st.error("Please select two different teams.")
        else:
            # Get the latest features for the selected teams
            home_team_id = teams_df[teams_df['team_name'] == home_team_selection]['team_id'].iloc[0]
            away_team_id = teams_df[teams_df['team_name'] == away_team_selection]['team_id'].iloc[0]
            
            # Find the most recent match for each team to get their latest stats
            home_stats = training_df[training_df['home_team_id'] == home_team_id].sort_values(by='match_date', ascending=False).iloc[0]
            away_stats = training_df[training_df['away_team_id'] == away_team_id].sort_values(by='match_date', ascending=False).iloc[0]

            # Prepare the feature vector for the model
            prediction_features = [
                home_stats['home_avg_goals_scored_last_5'],
                home_stats['home_avg_goals_conceded_last_5'],
                away_stats['away_avg_goals_scored_last_5'],
                away_stats['away_avg_goals_conceded_last_5']
            ]
            
            # Get prediction probabilities
            prediction_proba = model.predict_proba([prediction_features])[0]
            
            # Get the class labels from the model
            classes = model.classes_
            
            # Create a dictionary for easy lookup
            proba_dict = {cls: prob for cls, prob in zip(classes, prediction_proba)}

            home_win_prob = proba_dict.get('HOME_WIN', 0)
            away_win_prob = proba_dict.get('AWAY_WIN', 0)
            draw_prob = proba_dict.get('DRAW', 0)

            st.success("Prediction successful!")
            pcol1, pcol2, pcol3 = st.columns(3)
            pcol1.metric(f"{home_team_selection} (Home) Win", f"{home_win_prob:.0%}")
            pcol2.metric("Draw", f"{draw_prob:.0%}")
            pcol3.metric(f"{away_team_selection} (Away) Win", f"{away_win_prob:.0%}")


    # --- HISTORICAL DATA ANALYSIS ---
    st.header("Historical Data Analysis")
    # The rest of your existing dashboard code
    filtered_matches = matches_df
    if selected_team != "All Teams":
        filtered_matches = matches_df[
            (matches_df['home_team_name'] == selected_team) | 
            (matches_df['away_team_name'] == selected_team)
        ]
    
    st.dataframe(
        filtered_matches,
        column_config={ "match_id": None, "league_id": None, "season": None, "match_date": "Date", "match_status": None, "home_team_id": None, "away_team_id": None, "home_team_name": "Home Team", "away_team_name": "Away Team", "home_team_score": "Home Score", "away_team_score": "Away Score", },
        hide_index=True,
    )

except Exception as e:
    st.error(f"An error occurred: {e}")