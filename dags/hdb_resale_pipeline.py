"""HDB resale pipeline: data.gov.sg -> raw Postgres -> dbt (staging -> star schema) -> tests.

YOU write the logic in this file (Phases 1 and 4). The structure is here so the DAG
shows up in the UI; tasks raise NotImplementedError until you fill them in.
"""

from datetime import datetime, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

DATASET_ID = (
    "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"  # Resale flat prices, Jan 2017 onwards
)
DBT = "/opt/dbt-venv/bin/dbt"
DBT_DIR = "/opt/airflow/dbt"


@dag(
    schedule="@weekly",  # Sundays 00:00 UTC
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["hdb"],
)
def hdb_resale_pipeline():
    @task
    def extract() -> str:
        """Phase 1: pull the CURRENT and PREVIOUS month from the data.gov.sg API.

        Why two months: the data is monthly (a `month` column like "2026-10") but
        data.gov.sg updates it daily, so the current month keeps growing and late
        registrations can still land in last month. Each weekly run refreshes both.

        Hints:
          - Get the run's date from the context:
              from airflow.sdk import get_current_context
              ctx = get_current_context()
              run_date = ctx["logical_date"]   # the Sunday this run is for
            Work out the two months from run_date and log them.
          - Endpoint: https://data.gov.sg/api/action/datastore_search?resource_id=<DATASET_ID>
            Try it in your browser first. Look at how to filter by month and how paging works.
          - API key (optional, free): send header {"x-api-key": os.environ["DATA_GOV_SG_API_KEY"]}
            Rate limit is ~4 requests / 10 sec without a key: sleep between pages, retry on HTTP 429.
          - Save rows to /opt/airflow/data/resale_<run_date>.csv and return that path.
        """
        raise NotImplementedError("Phase 1: write extract()")

    @task
    def load(csv_path: str) -> int:
        """Phase 1: load the CSV into raw.resale_transactions. Must be IDEMPOTENT.

        Hints:
          - PostgresHook(postgres_conn_id="warehouse") gives you a connection.
          - Rerunning must NOT duplicate rows. Pattern: in ONE transaction, delete the
            rows for the months in this file, then insert the fresh rows.
          - Return the number of rows loaded (shows up in the task's XCom).
        """
        raise NotImplementedError("Phase 1: write load()")

    # Phase 4: once your dbt models + tests exist, this runs them after every load.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && {DBT} build",
    )

    load(extract()) >> dbt_build


hdb_resale_pipeline()
