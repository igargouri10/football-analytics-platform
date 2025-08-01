# Use the official Airflow image as the base
FROM apache/airflow:2.8.0-python3.11

# Switch to the root user to install system packages
USER root
# Install git, which is good practice for Airflow environments
RUN apt-get update && apt-get install -y --no-install-recommends git

# Switch back to the non-privileged airflow user
USER airflow
# Install the dbt-duckdb library
RUN pip install --no-cache-dir dbt-duckdb==1.7.1