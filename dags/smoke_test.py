"""Smoke test: proves Airflow can reach the warehouse and dbt is installed.

Trigger it once from the UI. Both tasks green = your environment works.
"""

from datetime import datetime, timezone

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["phase-0"],
)
def smoke_test():
    @task
    def check_warehouse() -> str:
        hook = PostgresHook(postgres_conn_id="warehouse")
        version = hook.get_first("select version()")[0]
        schemas = hook.get_records(
            "select schema_name from information_schema.schemata order by 1"
        )
        print(f"Connected: {version}")
        print(f"Schemas: {[s[0] for s in schemas]}")  # should include 'raw'
        return version

    dbt_connection = BashOperator(
        task_id="dbt_connection",
        bash_command="cd /opt/airflow/dbt && /opt/dbt-venv/bin/dbt debug --connection",
    )

    check_warehouse() >> dbt_connection


smoke_test()
