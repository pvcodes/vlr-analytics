"""
DAG: vlr_stats_scrape
"""

import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
GCP_PROJECT_ID = "vlr-analytics"
GCP_REGION = "asia-south1"
GCP_CONN_ID = "google_cloud_default"

CLOUD_RUN_JOB_NAME = "vlr-stats-scraper"

GCS_BUCKET = "vlr-data-lake"
GCS_BRONZE_PREFIX = "bronze"

METADATA_CONN_ID = "vlr_metadata_postgres"
METADATA_TABLE = "vlr_events_metadata"
METADATA_BATCH_SIZE = 50

DEFAULT_ARGS = {
    "owner": "pvcodes",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=2),
}

INTERVAL = "0 * * * *"


@dag(
    dag_id="vlr_stats_scrapper",
    schedule=INTERVAL,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=2,
    max_active_tasks=16,
    default_args=DEFAULT_ARGS,
    tags=["vlr", "scraping", "cloud-run"],
)
def vlr_stats_scrape_dag():

    # -----------------------------------------------------------------------
    # STEP 1: Read unscraped metadata rows
    # -----------------------------------------------------------------------
    read_unscraped_rows = SQLExecuteQueryOperator(
        task_id="read_unscraped_rows",
        conn_id=METADATA_CONN_ID,
        sql=f"""
            SELECT id, event_id, map_id, map_name, region_abbr, agent
            FROM {METADATA_TABLE}
            WHERE is_completed = TRUE AND is_scrapped = FALSE
            ORDER BY event_id, map_id, map_name, region_abbr, agent
            LIMIT {METADATA_BATCH_SIZE};
        """,
        handler=lambda cursor: cursor.fetchall(),
        return_last=True,
    )

    # -----------------------------------------------------------------------
    @task
    def format_rows(rows: list) -> list[dict]:
        if not rows:
            return []
        return [
            {
                "row_id": row[0],
                "event_id": row[1],
                "map_id": row[2],
                "map_name": row[3],
                "region": row[4],
                "agent": row[5],
            }
            for row in rows
        ]

    formatted_rows = format_rows(read_unscraped_rows.output)

    # -----------------------------------------------------------------------
    # STEP 2: Trigger Cloud Run (Mapped)
    # -----------------------------------------------------------------------
    @task(
        map_index_template="{{ task.op_kwargs['event_id'] }}-{{ task.op_kwargs['region'] }}-{{ task.op_kwargs['map_name'] }}-{{ task.op_kwargs['agent'] }}"
    )
    def trigger_cloud_run(
        row_id: int,
        event_id: int,
        map_id: int,
        map_name: str,
        region: str,
        agent: str,
    ) -> dict:

        from airflow.sdk import get_current_context
        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        from airflow.providers.google.cloud.hooks.cloud_run import CloudRunHook

        ctx = get_current_context()
        ds = ctx["ds"]

        object_path = (
            f"{GCS_BRONZE_PREFIX}/"
            f"event_id={event_id}/"
            f"region={region}/"
            f"map={map_name}/"
            f"agent={agent}/"
            f"snapshot_date={ds}/"
            f"data.csv"
        )

        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

        if gcs_hook.exists(bucket_name=GCS_BUCKET, object_name=object_path):
            log.info("GCS object already exists. Skipping Cloud Run: %s", object_path)
            return {
                "row_id": row_id,
                "object_path": object_path,
            }

        hook = CloudRunHook(gcp_conn_id=GCP_CONN_ID)
        hook.execute_job(
            job_name=CLOUD_RUN_JOB_NAME,
            project_id=GCP_PROJECT_ID,
            region=GCP_REGION,
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            f"--event_id={event_id}",
                            f"--map_id={map_id}",
                            f"--map_name={map_name}",
                            f"--region={region}",
                            f"--agent={agent}",
                            f"--destination_bucket_name={GCS_BUCKET}",
                            f"--snapshot_date={ds}",
                        ]
                    }
                ]
            },
        )

        return {
            "row_id": row_id,
            "object_path": object_path,
        }

    run_results = trigger_cloud_run.expand_kwargs(formatted_rows)

    # -----------------------------------------------------------------------
    # STEP 3: Wait for GCS Object (Mapped)
    # -----------------------------------------------------------------------
    @task
    def wait_for_gcs_file(result: dict) -> dict:

        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        import time

        object_path = result["object_path"]
        hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

        timeout = 900
        poke = 30
        waited = 0

        while waited < timeout:
            if hook.exists(bucket_name=GCS_BUCKET, object_name=object_path):
                return {"row_id": result["row_id"]}
            time.sleep(poke)
            waited += poke

        raise TimeoutError(f"{object_path} not found after {timeout}s")

    completed_rows = wait_for_gcs_file.expand(result=run_results)

    # -----------------------------------------------------------------------
    # STEP 4A: Collect Completed IDs
    # -----------------------------------------------------------------------
    @task
    def collect_completed_ids(rows: list[dict]) -> list[int]:
        return [r["row_id"] for r in rows if r]

    row_ids = collect_completed_ids(completed_rows)

    # -----------------------------------------------------------------------
    # STEP 4B: Bulk Update (Single DB Connection)
    # -----------------------------------------------------------------------
    @task
    def mark_rows_scraped_bulk(row_ids: list[int]):

        if not row_ids:
            return

        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=METADATA_CONN_ID)

        hook.run(
            sql=f"""
                UPDATE {METADATA_TABLE}
                SET
                    is_scrapped = TRUE,
                    last_scraped = NOW()
                WHERE id = ANY(%(ids)s)
                  AND is_completed = TRUE;
            """,
            parameters={"ids": row_ids},
            autocommit=True,
        )

    mark_rows_scraped_bulk(row_ids)


vlr_stats_scrape_dag()
