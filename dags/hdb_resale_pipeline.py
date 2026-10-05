"""HDB resale pipeline: data.gov.sg -> raw Postgres -> dbt (staging -> star schema) -> tests.

Phase 1 (this file): extract the current + previous month from the API, then load them
into raw.resale_transactions idempotently. Phase 4 adds dbt models/tests after load.
"""

import csv
import json
import logging
import os
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, get_current_context, task

DATASET_ID = (
    "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"  # Resale flat prices, Jan 2017 onwards
)
API_URL = "https://data.gov.sg/api/action/datastore_search"
PAGE_SIZE = 1000
DATA_DIR = "/opt/airflow/data"
SG_TZ = ZoneInfo("Asia/Singapore")  # the source's `month` column is Singapore time

# Source columns, in API order. Kept as TEXT in raw; dbt staging does the casting.
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

DBT = "/opt/dbt-venv/bin/dbt"
DBT_DIR = "/opt/airflow/dbt"

log = logging.getLogger(__name__)


def months_to_load(run_date: datetime) -> list[str]:
    """Previous and current month (YYYY-MM) for a run date, in Singapore time."""
    local = run_date.astimezone(SG_TZ)
    year, month = local.year, local.month
    prev_year, prev_month = (year - 1, 12) if month == 1 else (year, month - 1)
    return [f"{prev_year}-{prev_month:02d}", f"{year}-{month:02d}"]


def fetch_month(session: requests.Session, month: str) -> list[dict]:
    """All records for one month, paging through the API with rate-limit handling."""
    rows: list[dict] = []
    offset = 0
    while True:
        params = {
            "resource_id": DATASET_ID,
            "filters": json.dumps({"month": month}),
            "limit": PAGE_SIZE,
            "offset": offset,
        }
        for attempt in range(1, 6):
            resp = session.get(API_URL, params=params, timeout=60)
            if resp.status_code != 429:
                break
            wait = 10 * attempt
            log.warning(
                "429 rate limited on %s offset %s, sleeping %ss", month, offset, wait
            )
            time.sleep(wait)
        resp.raise_for_status()
        body = resp.json()
        if not body.get("success"):
            raise RuntimeError(f"API error for {month}: {body}")

        result = body["result"]
        records = result["records"]
        rows.extend(records)
        offset += len(records)
        if not records or offset >= result["total"]:
            return rows
        time.sleep(3)  # ~4 requests / 10s without an API key


@dag(
    schedule="@weekly",  # Sundays 00:00 UTC
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["hdb"],
)
def hdb_resale_pipeline():
    @task
    def extract() -> dict:
        """Pull the CURRENT and PREVIOUS month from data.gov.sg into a CSV.

        The data is monthly but updated daily, so the current month keeps growing and
        late registrations can still land in last month. Each run refreshes both.
        """
        ctx = get_current_context()
        # Manual runs can have no logical_date in Airflow 3; fall back to run_after.
        run_date = ctx.get("logical_date") or ctx["dag_run"].run_after
        months = months_to_load(run_date)
        log.info("Run date %s -> extracting months %s", run_date, months)

        session = requests.Session()
        api_key = os.environ.get("DATA_GOV_SG_API_KEY")
        if api_key:
            session.headers["x-api-key"] = api_key

        os.makedirs(DATA_DIR, exist_ok=True)
        csv_path = f"{DATA_DIR}/resale_{run_date:%Y%m%dT%H%M%S}.csv"
        total = 0
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for month in months:
                rows = fetch_month(session, month)
                writer.writerows(rows)
                total += len(rows)
                log.info("%s: %s rows", month, len(rows))

        log.info("Wrote %s rows to %s", total, csv_path)
        return {"csv_path": csv_path, "months": months}

    @task
    def load(extracted: dict) -> int:
        """Load the CSV into raw.resale_transactions, IDEMPOTENTLY.

        In one transaction: delete the months being refreshed, then COPY the fresh
        rows in. Reruns replace rows instead of duplicating them, and a failure
        rolls back so the table never holds a half-loaded month.
        """
        csv_path, months = extracted["csv_path"], extracted["months"]
        cols = ", ".join(COLUMNS)

        conn = PostgresHook(postgres_conn_id="warehouse").get_conn()
        try:
            # `with conn` commits on success, or rolls back on error
            with conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    create table if not exists raw.resale_transactions (
                        {", ".join(f"{c} text" for c in COLUMNS)},
                        loaded_at timestamp not null default now()
                    )
                    """
                )
                cur.execute(
                    "delete from raw.resale_transactions where month = any(%s)",
                    (months,),
                )
                log.info("Deleted %s existing rows for %s", cur.rowcount, months)
                with open(csv_path) as f:
                    cur.copy_expert(
                        f"copy raw.resale_transactions ({cols}) from stdin with csv header",
                        f,
                    )
                cur.execute(
                    "select count(*) from raw.resale_transactions where month = any(%s)",
                    (months,),
                )
                loaded = cur.fetchone()[0]
        finally:
            conn.close()

        log.info("Loaded %s rows for %s", loaded, months)
        return loaded

    # Phase 4: once your dbt models + tests exist, this runs them after every load.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && {DBT} build",
    )

    load(extract()) >> dbt_build


hdb_resale_pipeline()
