"""
fact_trips_via_staging — a scalable alternative to ride_db_to_dw.py.

ride_db_to_dw.py bundles extract + load into one task per dimension, and
extract + transform + quality + load into ONE task for fact_trips, to
avoid passing row-level data through XCom (see fact_trips_via_xcom.py for
why XCom-ing full datasets doesn't scale). The tradeoff: a single task
means a red box in the UI tells you nothing about which stage failed, and
any retry re-runs the whole thing — including a fresh hit against the
source OLTP database — even if only the load step failed.

This DAG keeps the "don't put row-level data through Airflow's metadata
DB" property, but still gets per-stage retries and observability, by
routing data through ordinary staging tables in the warehouse instead of
through XCom — for every table, not just fact_trips:

    extract_<dim>_to_staging   OLTP <dim>        -> stg_dim_<dim>
    load_<dim>_from_staging    stg_dim_<dim>     -> dim_<dim>
    extract_trips_to_staging   OLTP trips        -> stg_trip_extract
    transform_trips_staged     stg_trip_extract  -> stg_fact_trips
    quality_check_trips_staged stg_fact_trips    (read-only gate)
    load_trips_staged_to_fact  stg_fact_trips    -> fact_trips
    cleanup_staging             deletes this run's staging rows (always runs)

Only a small string — the Airflow run_id, used as a batch_id to scope each
run's rows — ever crosses a task boundary. Every task derives it from its
own context instead of pulling it from XCom, so XCom carries zero pipeline
data end to end.

Benefits over the single-task design:
  - A failure in transform/quality/load doesn't re-extract from the source.
  - The quality gate is its own task: a genuine data-quality failure fails
    fast (AirflowFailException, no retry) instead of being retried twice
    like a transient connection error would be.
  - A dimension's extract and load are separately retryable too — a load
    failure doesn't re-hit the OLTP database.
  - The UI shows exactly which stage failed.

Cost: staging tables to maintain, a cleanup task that must run even on
failure (trigger_rule="all_done") so staging rows don't accumulate forever,
and extra round trips to Postgres per row. For this course's data volumes
that cost is trivial; at real scale it's a legitimate pattern (sometimes
called "extract-load-transform" staging).

The five dimensions are wired identically (extract to a staging table,
load from it), so their tasks are generated from DIM_CONFIGS below instead
of hand-copied five times — each still gets its own distinct, independently
retryable task_id in the UI (this is a plain loop calling a task factory,
not Airflow's dynamic task mapping).

All staging-table DDL, SQL, and stage/read helpers live in pipeline/staging.py
— this module only wires tasks together and owns connection lifecycle.
"""

from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, Param, task
from airflow.sdk.exceptions import AirflowFailException

from pipeline.extract import (
    extract_trips_full,
    extract_trips_incremental,
    extract_lookup_dim,
    get_watermark,
)
from pipeline.load import load_fact_trips
from pipeline.transform import transform
from pipeline.quality import run_quality_checks, DataQualityError
from pipeline.staging import (
    DIM_CONFIGS,
    STAGING_TABLES_SQL,
    stage_dim_rows,
    read_dim_staged_rows,
    stage_trip_extract_rows,
    read_trip_extract_rows,
    stage_fact_trips_rows,
    read_fact_trips_rows,
    cleanup_staging_rows,
)

SRC_CONN_ID = "ride_sharing_oltp"
DEST_CONN_ID = "ride_sharing_warehouse"


with DAG(
    dag_id="fact_trips_via_staging",
    description="Scalable alternative to ride_db_to_dw.py: dims and fact_trips both "
    "extracted/transformed/loaded as separate tasks, using warehouse staging tables "
    "(not XCom) to move data between them",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        "full_reload": Param(
            False,
            type="boolean",
            description="Truncate-reload every trip instead of the default incremental load",
        ),
    },
    tags=["rides", "etl", "staging-pattern"],
) as dag:

    create_staging_tables = SQLExecuteQueryOperator(
        task_id="create_staging_tables",
        conn_id=DEST_CONN_ID,
        sql=STAGING_TABLES_SQL,
    )

    def make_dim_staging_tasks(cfg):
        """Build the extract-to-staging / load-from-staging task pair for one
        dimension. A function call (not a bare loop body) so each pair closes
        over its own cfg — avoids the classic late-binding closure bug.
        """
        staging_table = cfg["staging_table"]
        columns = cfg["columns"]
        extract_fn = cfg["extract_fn"]
        load_fn = cfg["load_fn"]

        @task(task_id=f"extract_{cfg['key']}_to_staging")
        def extract_dim_to_staging(**context):
            batch_id = context["run_id"]
            src = PostgresHook(SRC_CONN_ID).get_conn()
            dst = PostgresHook(DEST_CONN_ID).get_conn()
            try:
                rows = [dict(r) for r in extract_fn(src)]
                stage_dim_rows(dst, staging_table, columns, rows, batch_id)
            finally:
                src.close()
                dst.close()

        @task(task_id=f"load_{cfg['key']}_from_staging")
        def load_dim_from_staging(**context):
            batch_id = context["run_id"]
            dst = PostgresHook(DEST_CONN_ID).get_conn()
            try:
                rows = read_dim_staged_rows(dst, staging_table, batch_id)
                load_fn(dst, rows)
            finally:
                dst.close()

        extract_task = extract_dim_to_staging()
        load_task = load_dim_from_staging()
        create_staging_tables >> extract_task >> load_task
        return load_task

    dim_load_tasks = [make_dim_staging_tasks(cfg) for cfg in DIM_CONFIGS]

    @task
    def extract_trips_to_staging(**context):
        batch_id = context["run_id"]
        full_reload = context["params"]["full_reload"]

        src = PostgresHook(SRC_CONN_ID).get_conn()
        dst = PostgresHook(DEST_CONN_ID).get_conn()
        try:
            if full_reload:
                rows = extract_trips_full(src)
            else:
                watermark = get_watermark(dst)
                rows = extract_trips_incremental(src, {"watermark": watermark})

            stage_trip_extract_rows(dst, rows, batch_id)
        finally:
            src.close()
            dst.close()

    @task
    def transform_trips_staged(**context):
        batch_id = context["run_id"]

        dst = PostgresHook(DEST_CONN_ID).get_conn()
        try:
            rows = read_trip_extract_rows(dst, batch_id)
            lookups = extract_lookup_dim(dst)
            fact_rows = transform(rows, lookups)
            stage_fact_trips_rows(dst, fact_rows, batch_id)
        finally:
            dst.close()

    @task
    def quality_check_trips_staged(**context):
        batch_id = context["run_id"]

        dst = PostgresHook(DEST_CONN_ID).get_conn()
        try:
            fact_rows = read_fact_trips_rows(dst, batch_id)
        finally:
            dst.close()

        try:
            run_quality_checks(fact_rows)
        except DataQualityError as e:
            # A bad batch won't pass on retry — fail fast instead of
            # burning the DAG's retry budget on a non-transient error.
            raise AirflowFailException(str(e)) from e

    @task
    def load_trips_staged_to_fact(**context):
        batch_id = context["run_id"]
        full_reload = context["params"]["full_reload"]

        dst = PostgresHook(DEST_CONN_ID).get_conn()
        try:
            fact_rows = read_fact_trips_rows(dst, batch_id)
            load_fact_trips(dst, fact_rows, full_reload=full_reload)
        finally:
            dst.close()

    @task(trigger_rule="all_done")
    def cleanup_staging(**context):
        batch_id = context["run_id"]

        dst = PostgresHook(DEST_CONN_ID).get_conn()
        try:
            cleanup_staging_rows(dst, batch_id)
        finally:
            dst.close()

    extract_trips_task = extract_trips_to_staging()
    transform_task = transform_trips_staged()
    quality_task = quality_check_trips_staged()
    load_trips_task = load_trips_staged_to_fact()

    # Trip extraction only needs the staging tables, not the dims — it can
    # run in parallel with the dim pipelines. Transform needs both: the
    # staged trip rows AND the dims loaded (it looks up dim keys via
    # extract_lookup_dim), so it's gated on both.
    create_staging_tables >> extract_trips_task
    [extract_trips_task, *dim_load_tasks] >> transform_task >> quality_task >> load_trips_task
    load_trips_task >> cleanup_staging()
