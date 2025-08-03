# dashboard/app.py - FINAL SNOWFLAKE VERSION

import streamlit as st
import pandas as pd
import snowflake.connector
import os
import joblib

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Football Analytics Dashboard",
    page_icon="⚽",
    layout="wide",
)

# --- SNOWFLAKE CONNECTION ---
# Use st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    """Initializes a connection to Snowflake, reading credentials from Streamlit secrets."""
    creds = st.secrets["snowflake"]
    return snowflake.connector.connect(
        user=creds["user"],
        password=creds["password"],
        account=creds["account"],
        warehouse=creds["warehouse"],
        database=creds["database"],
        schema=creds["schema"]
    )

# --- DATA LOADING ---
# Use st.cache_data to cache data loading for 10 minutes.
@st.cache_data(ttl=600)
def load_data(_conn):
    """Loads tables from Snowflake and returns them as pandas DataFrames."""
    cursor = _conn.cursor()
    
    # Load tables
    cursor.execute("SELECT * FROM PROD.RAW.FCT_MATCHES")
    matches_df = cursor.fetch_pandas_all()
    
    cursor.execute("SELECT TEAM_ID, TEAM_NAME FROM PROD.RAW.DIM_TEAMS ORDER BY TEAM_NAME ASC")
    teams_df = cursor.fetch_pandas_all()

    cursor.execute("SELECT * FROM PROD.RAW.FCT_TRAINING_DATASET")
    training_df = cursor.fetch_pandas_all()
    
    cursor.close()
    
    # Convert column names to lowercase for consistency
    matches_df.columns = matches_df.columns.str.lower()
    teams_df.columns = teams_df.columns.str.lower()
    training_df.columns = training_df.columns.str.lower()
    
    matches_df['match_date'] = pd.to_datetime(matches_df['match_date'])
    
    return matches_df, teams_df, training_df

# --- MODEL LOADING ---
@st.cache_resource
def load_model():
    """Loads the trained model from the .pkl file."""
    # This part remains the same, loading a local file
    model_path = os.path.join(os.path.dirname(__file__), 'match_predictor.pkl')
    model = joblib.load(model_path)
    return model

# --- MAIN APPLICATION LOGIC ---
try:
    conn = init_connection()
    matches_df, teams_df, training_df = load_data(conn)
    model = load_model()

    # --- UI LAYOUT ---
    st.title("⚽ Football Analytics Dashboard")
    st.markdown("An interactive dashboard to explore match results and predict outcomes, powered by Snowflake, dbt, and Airflow.")

    # Your existing UI code for the AI predictor and historical analysis will now work
    # with the data loaded from Snowflake. The code below is the same as your last version.

    # --- SIDEBAR FOR TEAM SELECTION ---
    st.sidebar.header("Filter by Team")
    team_list = ["All Teams"] + teams_df['team_name'].tolist()
    selected_team = st.sidebar.selectbox("Select a Team", team_list)

    # --- MAIN PAGE ---
    # --- AI MATCH PREDICTOR ---
    st.header("🔮 AI Match Predictor")
    
    team_names = teams_df['team_name'].tolist()
    col1, col2 = st.columns(2)
    home_team_selection = col1.selectbox("Select Home Team", team_names, index=team_names.index("Arsenal") if "Arsenal" in team_names else 0)
    away_team_selection = col2.selectbox("Select Away Team", team_names, index=team_names.index("Aston Villa") if "Aston Villa" in team_names else 1)

    if st.button("Predict Outcome"):
        if home_team_selection == away_team_selection:
            st.error("Please select two different teams.")
        else:
            home_team_id = teams_df[teams_df['team_name'] == home_team_selection]['team_id'].iloc[0]
            away_team_id = teams_df[teams_df['team_name'] == away_team_selection]['team_id'].iloc[0]
            
            home_stats = training_df[training_df['home_team_id'] == home_team_id].sort_values(by='match_date', ascending=False).iloc[0]
            away_stats = training_df[training_df['away_team_id'] == away_team_id].sort_values(by='match_date', ascending=False).iloc[0]

            prediction_features = [[
                home_stats['home_avg_goals_scored_last_5'],
                home_stats['home_avg_goals_conceded_last_5'],
                away_stats['away_avg_goals_scored_last_5'],
                away_stats['away_avg_goals_conceded_last_5']
            ]]
            
            prediction_proba = model.predict_proba(prediction_features)[0]
            classes = model.classes_
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
    if selected_team == "All Teams":
        filtered_matches = matches_df
    else:
        team_id = teams_df[teams_df['team_name'] == selected_team]['team_id'].iloc[0]
        filtered_matches = matches_df[
            (matches_df['home_team_id'] == team_id) | 
            (matches_df['away_team_id'] == team_id)
        ]
    
    st.dataframe(
        filtered_matches,
        column_config={
            "match_id": None, "league_id": None, "season": None, "match_date": "Date", 
            "match_status": None, "home_team_id": None, "away_team_id": None,
            "home_team_name": "Home Team", "away_team_name": "Away Team",
            "home_team_score": "Home Score", "away_team_score": "Away Score",
        },
        hide_index=True,
    )

except Exception as e:
    st.error(f"An error occurred: {e}")