# End-to-End Football Analytics Platform

![Streamlit Dashboard GIF](httpsor_your_gif_here.gif) <!-- **IMPORTANT**: Create a GIF of your dashboard and replace this line! -->

This project is a complete, end-to-end data platform that automates the process of ingesting real-time football (soccer) match data, transforming it into a reliable analytical warehouse, and presenting the insights through an interactive web dashboard.

### Key Features

*   **Automated Ingestion:** An Apache Airflow DAG running in Docker automatically fetches new match data daily from a live API.
*   **Cloud Data Lake:** Raw JSON data is reliably stored in an AWS S3 bucket, creating a scalable and durable data lake.
*   **Modern Data Transformation:** dbt is used to transform the raw data into a clean, tested, and de-duplicated star schema (facts and dimensions) powered by DuckDB.
*   **Data Quality Assurance:** The dbt pipeline includes automated tests to ensure the uniqueness and integrity of key data points like `match_id` and `team_id`.
*   **Interactive Dashboard:** A Streamlit application provides a user-friendly interface to filter and analyze the data, with metrics and charts that update dynamically.

### Architecture Diagram

The platform follows a modern ELT (Extract, Load, Transform) architecture.

![Architecture Diagram](httpsor_your_diagram_here.png) <!-- **IMPORTANT**: Create a simple diagram and replace this line! -->

**Data Flow:**
`API -> Python Script -> Airflow (Docker) -> AWS S3 (Data Lake) -> dbt -> DuckDB (Data Warehouse) -> Streamlit (Dashboard)`

### Tech Stack

| Category              | Technology                                   |
| --------------------- | -------------------------------------------- |
| **Orchestration**     | Apache Airflow, Docker, Docker Compose       |
| **Data Ingestion**    | Python (`requests`, `boto3`)                 |
| **Data Lake**         | AWS S3                                       |
| **Transformation**    | dbt (Data Build Tool)                        |
| **Data Warehouse**    | DuckDB                                       |
| **Dashboarding**      | Streamlit, Pandas                            |
| **Infrastructure**    | AWS IAM (for secure access)                  |
| **Version Control**   | Git & GitHub                                 |

### Local Setup and Installation

To run this project on your own machine, please follow these steps.

**Prerequisites:**
*   Python 3.9+
*   Docker Desktop
*   An AWS Account with an S3 bucket and an IAM user with programmatic access.
*   An API Token from [football-data.org](https://www.football-data.org/).

**1. Clone the Repository**
```bash
git clone https://github.com/your-username/football-analytics-platform.git
cd football-analytics-platform
```

**2. Create a Python Virtual Environment**
```bash
python -m venv .venv
.\.venv\Scripts\activate
```

**3. Install Dependencies**
```bash
pip install -r requirements.txt 
# Note: You will need to create a requirements.txt file with:
# streamlit, pandas, duckdb, dbt-core, dbt-duckdb, requests, boto3, python-dotenv
```

**4. Configure Environment Variables**
Create a `.env` file in the project root and populate it with your credentials. **This file should be in your `.gitignore` and never committed.**
```env
# .env file
API_FOOTBALL_TOKEN=YOUR_FOOTBALL_API_TOKEN
AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY
AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_KEY
AWS_S3_BUCKET_NAME=your-s3-bucket-name
AWS_S3_REGION=us-east-1
```

**5. Configure dbt Profile**
Create a `profiles.yml` file at `~/.dbt/` (`C:\Users\YourUsername\.dbt\` on Windows) with the following content:
```yaml
football_analytics:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: target/dbt.duckdb
      # Add other S3 settings if needed, but environment variables should work
```

### How to Run the Project

1.  **Start Airflow:** From the project root, start the Docker services.
    ```bash
    docker-compose up -d --build
    ```
2.  **Initialize Airflow (First Time Only):**
    ```bash
    docker-compose exec airflow-webserver airflow db init
    docker-compose exec airflow-webserver airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
    ```
3.  **Run the Ingestion DAG:** Go to `http://localhost:8080`, log in, and trigger the `football_data_ingestion` DAG to populate your S3 bucket.

4.  **Run the Transformation Pipeline:**
    *   Open a new terminal and activate the virtual environment (`.\.venv\Scripts\activate`).
    *   Load your secrets from the `.env` file.
    *   Navigate to the dbt project: `cd dbt_project`.
    *   Build and test your data warehouse:
    ```bash
    dbt run
    dbt test
    ```

5.  **Launch the Dashboard:**
    *   Navigate back to the project root (`cd ..`).
    *   Run the Streamlit app:
    ```bash
    streamlit run dashboard/app.py
    ```
    Your dashboard will open at `http://localhost:8501`.