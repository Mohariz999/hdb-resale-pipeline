"""HDB resale pipeline: data.gov.sg -> raw Postgres -> dbt (staging -> star schema) -> tests.

Weekly runs refresh a rolling two-month window. A manual run with start_month (and
optionally end_month) set backfills any range instead, e.g. all history from 2017-01.
"""

import csv
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, get_current_context, task

log = logging.getLogger(__name__)

DATASET_ID = (
    "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"  # Resale flat prices, Jan 2017 onwards
)
API_URL = "https://data.gov.sg/api/action/datastore_search"
PAGE_SIZE = 1000
MAX_RETRIES = 5
DATA_DIR = "/opt/airflow/data"
DBT = "/opt/dbt-venv/bin/dbt"
DBT_DIR = "/opt/airflow/dbt"

# Source columns, in a fixed order so the CSV and the table always line up.
COLUMNS = [
    "month",
    "town",
    "flat_type",
    "block",
    "street_name",
    "storey_range",
    "floor_area_sqm",
    "flat_model",
    "lease_commence_date",
    "remaining_lease",
    "resale_price",
]

# Everything is TEXT in raw: load exactly what the source sent, cast types later in dbt.
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw.resale_transactions (
    month TEXT,
    town TEXT,
    flat_type TEXT,
    block TEXT,
    street_name TEXT,
    storey_range TEXT,
    floor_area_sqm TEXT,
    flat_model TEXT,
    lease_commence_date TEXT,
    remaining_lease TEXT,
    resale_price TEXT,
    loaded_at TIMESTAMP NOT NULL DEFAULT now()
)
"""


def months_to_refresh(run_date: datetime) -> list[str]:
    """The run's month and the month before it, as "YYYY-MM"."""
    first_of_month = run_date.replace(day=1)
    previous = first_of_month - timedelta(days=1)  # also handles Jan -> Dec
    return [previous.strftime("%Y-%m"), run_date.strftime("%Y-%m")]


def months_between(start: str, end: str) -> list[str]:
    """Every month from start to end inclusive, as "YYYY-MM"."""
    year, month = map(int, start.split("-"))
    months = []
    while f"{year:04d}-{month:02d}" <= end:
        months.append(f"{year:04d}-{month:02d}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    if not months:
        raise ValueError(f"start_month {start} is after end_month {end}")
    return months


def fetch_month(month: str, headers: dict, pause: float) -> list[dict]:
    """Page through the API for one month and return all its records."""
    records: list[dict] = []
    offset = 0
    while True:
        params = {
            "resource_id": DATASET_ID,
            "filters": f'{{"month": "{month}"}}',
            "limit": PAGE_SIZE,
            "offset": offset,
        }
        for attempt in range(1, MAX_RETRIES + 1):
            resp = requests.get(API_URL, params=params, headers=headers, timeout=30)
            if resp.status_code != 429:
                break
            wait = pause * 2**attempt  # rate limited: back off and try again
            log.warning("429 from data.gov.sg, retry %s in %.0fs", attempt, wait)
            time.sleep(wait)
        resp.raise_for_status()  # still 429 after retries, or any other error: fail the task

        body = resp.json()
        if not body.get("success"):
            raise RuntimeError(f"data.gov.sg error for {month}: {body}")
        result = body["result"]
        records.extend(result["records"])

        offset += PAGE_SIZE
        if offset >= result["total"]:
            return records
        time.sleep(pause)  # stay under the rate limit between pages


@dag(
    schedule="@weekly",  # Sundays 00:00 UTC
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["hdb"],
    # Every task retries twice, 5 minutes apart, before the run is marked failed. That rides
    # out a brief API or network blip without anyone having to rerun it by hand.
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    # Leave both empty for the normal rolling window. To backfill, use "Trigger DAG"
    # with config, e.g. {"start_month": "2017-01"}; end_month defaults to the run's month.
    params={
        "start_month": Param("", type="string", pattern=r"^(\d{4}-\d{2})?$"),
        "end_month": Param("", type="string", pattern=r"^(\d{4}-\d{2})?$"),
    },
)
def hdb_resale_pipeline():
    @task
    def extract() -> str:
        """Pull the months to refresh from data.gov.sg into a CSV.

        Normally the current and previous month: the data is monthly but data.gov.sg
        updates it daily, so the current month keeps growing and late registrations can
        still land in last month. With start_month set, every month in the range instead.
        """
        ctx = get_current_context()
        # Manual triggers can have no logical_date in Airflow 3, so fall back to run_after.
        run_date = ctx.get("logical_date") or ctx["dag_run"].run_after
        start, end = ctx["params"]["start_month"], ctx["params"]["end_month"]
        if start:
            months = months_between(start, end or run_date.strftime("%Y-%m"))
            log.info(
                "Backfilling %s months, %s to %s", len(months), months[0], months[-1]
            )
        else:
            months = months_to_refresh(run_date)
            log.info("Refreshing months %s (run date %s)", months, run_date)

        api_key = os.environ.get("DATA_GOV_SG_API_KEY")
        headers = {"x-api-key": api_key} if api_key else {}
        pause = 1.5 if api_key else 3.0  # ~8 vs ~4 requests per 10 sec

        path = f"{DATA_DIR}/resale_{run_date:%Y%m%dT%H%M%S}.csv"
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for month in months:
                rows = fetch_month(month, headers, pause)
                writer.writerows(rows)  # extrasaction="ignore" drops the API's _id
                log.info("%s: %s rows", month, len(rows))
                time.sleep(pause)
        return path

    @task
    def load(csv_path: str) -> int:
        """Load the CSV into raw.resale_transactions, idempotently.

        In ONE transaction: delete the rows for the months in this file, then insert the
        fresh rows. A rerun replaces those months instead of adding duplicates, and if
        anything fails the whole load rolls back, so the table is never half-updated.
        """
        with open(csv_path, newline="") as f:
            rows = [tuple(r[c] for c in COLUMNS) for r in csv.DictReader(f)]
        months = sorted({r[0] for r in rows})
        if not rows:
            log.warning("No rows in %s, nothing to load", csv_path)
            return 0

        conn = PostgresHook(postgres_conn_id="warehouse").get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(CREATE_TABLE_SQL)
                cur.execute(
                    "DELETE FROM raw.resale_transactions WHERE month = ANY(%s)",
                    (months,),
                )
                log.info("Deleted %s old rows for %s", cur.rowcount, months)
                placeholders = ", ".join(["%s"] * len(COLUMNS))
                cur.executemany(
                    f"INSERT INTO raw.resale_transactions ({', '.join(COLUMNS)}) "
                    f"VALUES ({placeholders})",
                    rows,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        log.info("Loaded %s rows for %s", len(rows), months)
        return len(rows)

    # Builds every model and runs every test. A failed test fails this task, and the run.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && {DBT} build",
    )

    load(extract()) >> dbt_build


hdb_resale_pipeline()
