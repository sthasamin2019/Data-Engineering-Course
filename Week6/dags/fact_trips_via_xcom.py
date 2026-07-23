"""
fact_trips_via_xcom — ANTI-PATTERN DEMO, not a template to copy.

Same fact_trips slice of the pipeline as ride_db_to_dw.py (extract trips ->
transform -> load), but wired the "obvious" TaskFlow way: each stage returns
its full row-level dataset, and Airflow XComs it to the next task instead of
keeping the data in-process.

Why this doesn't scale:
  1. Every XCom value is serialized to JSON and written as a row in the
     Airflow metadata DB (xcom table), then read back and deserialized by
     the next task. A 10k-row trip extract here is a few MB; a real fact
     table extract (millions of rows, back-filled) would be hundreds of MB
     to GBs sitting in Postgres next to your scheduler's own state.
  2. That serialize/deserialize happens at EVERY task boundary — extract to
     transform, transform to load — on top of the actual work. Double the
     data hops, double the CPU/network spent moving bytes nobody but the
     next task needs.
  3. The whole dataset must fit in one worker's memory at once, twice over
     (once to build it, once to hold the deserialized copy pulled from
     XCom). No chunking, no streaming — extract_trips_full() below already
     loads all rows into a Python list before XCom can even see it.
  4. The metadata DB backs the scheduler, the UI, DAG parsing, task
     history — everything. Bloating it with pipeline payloads risks
     slowing down or destabilizing the whole Airflow instance, not just
     this DAG.
  5. It doesn't compose: try turning this into a per-partition dynamic
     task-mapped DAG and you get one XCom row per partition, all still
     round-tripping through the same metadata DB.

Compare with ride_db_to_dw.py, where extract -> transform -> quality ->
load all happen inside ONE task against in-memory Python objects, and
nothing but a per-run log line ever touches Airflow's metadata store. If
you outgrow that, the real fix is external storage (write extracted/
transformed data to S3/GCS/a staging table) and pass a *pointer* (a file
path, a table name) through XCom — not the data itself.
"""

import json
import logging
from datetime import datetime, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, task

from pipeline.extract import extract_trips_full, extract_lookup_dim
from pipeline.load import load_fact_trips
from pipeline.transform import transform
from pipeline.quality import run_quality_checks

logger = logging.getLogger(__name__)

SRC_CONN_ID = "ride_sharing_oltp"
DEST_CONN_ID = "ride_sharing_warehouse"


with DAG(
    dag_id="fact_trips_via_xcom",
    description="ANTI-PATTERN DEMO: extract -> transform -> load for fact_trips, "
    "with row-level data passed through XCom between tasks",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["rides", "etl", "antipattern", "demo"],
) as dag:

    @task
    def extract_trips():
        src = PostgresHook(SRC_CONN_ID).get_conn()
        try:
            rows = [dict(r) for r in extract_trips_full(src)]
        finally:
            src.close()

        payload_bytes = len(json.dumps(rows, default=str).encode())
        logger.warning(
            f"XCom-ing {len(rows)} trip rows to 'transform_trips' — "
            f"~{payload_bytes / 1_000_000:.2f} MB written to the Airflow metadata DB"
        )
        return rows

    @task
    def transform_trips(rows: list):
        dst = PostgresHook(DEST_CONN_ID).get_conn()
        try:
            lookups = extract_lookup_dim(dst)
        finally:
            dst.close()

        fact_rows = transform(rows, lookups)

        payload_bytes = len(json.dumps(fact_rows, default=str).encode())
        logger.warning(
            f"XCom-ing {len(fact_rows)} fact rows to 'load_trips' — "
            f"~{payload_bytes / 1_000_000:.2f} MB written to the Airflow metadata DB"
        )
        return fact_rows

    @task
    def load_trips(fact_rows: list):
        run_quality_checks(fact_rows)
        dst = PostgresHook(DEST_CONN_ID).get_conn()
        try:
            load_fact_trips(dst, fact_rows, full_reload=True)
        finally:
            dst.close()

    load_trips(transform_trips(extract_trips()))
