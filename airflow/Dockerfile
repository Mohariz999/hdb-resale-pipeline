FROM apache/airflow:3.3.2-python3.12

# dbt gets its own virtualenv so its dependencies never clash with Airflow's.
# Call it as /opt/dbt-venv/bin/dbt
USER root
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-postgres==1.11.0" \
    && chmod -R a+rX /opt/dbt-venv
USER airflow
