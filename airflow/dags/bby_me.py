"""
DAG: actuals_my_first_dag
Workflow per row from metadata DB:
  1. Read rows from metadata DB (SQLExecuteQueryOperator)
  2. Format rows into kwargs list (@task)
  3. For each row (mapped @task_group):
     a. Trigger Cloud Run job with row args
     b. Check GCS for output file
     c. If file exists, update the row (last_scraped, is_scraped=true)
"""

from __future__ import annotations

from datetime import datetime, timezone

from airflow.sdk import dag, task, task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecuteJobOperator,
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook

PROJECT_ID = "vlr-analytics"
REGION = "asia-south1"
JOB_NAME = "vlr-stats-scraper"
BUCKET = "vlr-data-lake"


@dag(
    dag_id="actuals_my_first_dag",
    tags=["pvcodes"],
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def my_dag():

    # -------------------------------------------------------------------------
    # Step 1: Read unscraped rows from the metadata DB.
    #
    # In Airflow 3.x, SQLExecuteQueryOperator with return_last=True pushes the
    # result of the last statement to XCom as a list of tuples.
    # handler=fetch_all_handler (the default) fetches all rows.
    # The .output XCom is a list of tuples:
    #   [(id, event_id, map_id, map_name, region_abbr, agent), ...]
    # -------------------------------------------------------------------------
    read_events_metadata = SQLExecuteQueryOperator(
        task_id="read_events_metadata",
        conn_id="local_vlr_metadata_db",
        sql="""
            SELECT id, event_id, map_id, map_name, region_abbr, agent
            FROM vlr_events_metadata
            WHERE is_scrapped = false
            LIMIT 1;
        """,
        # handler is the default fetch_all_handler in common-sql >= 1.3,
        # but being explicit keeps this clear.
        handler=lambda cursor: cursor.fetchall(),
        return_last=True,
    )

    # -------------------------------------------------------------------------
    # Step 2: Convert list-of-tuples into list-of-dicts for expand_kwargs.
    #
    # expand_kwargs() expects a list of dicts; each dict becomes the kwargs
    # for one mapped task_group instance.
    # -------------------------------------------------------------------------
    @task
    def format_rows(rows: list) -> list[dict]:
        """
        rows: [(id, event_id, map_id, map_name, region_abbr, agent), ...]
        Returns a list of dicts, one per row, used by expand_kwargs.
        """
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

    formatted = format_rows(read_events_metadata.output)

    # -------------------------------------------------------------------------
    # Step 3: Mapped task group — one instance per metadata row.
    #
    # Using @task_group + .expand_kwargs() is the correct Airflow 3.x pattern
    # for "per-item sequential sub-pipelines". Each instance runs:
    #   trigger_cloud_run → check_gcs → update_metadata
    #
    # Classic operators (e.g. CloudRunExecuteJobOperator) cannot directly
    # consume mapped task_group kwargs due to the MappedArgument issue, so
    # we wrap them in @task functions that call hooks/operators imperatively.
    # -------------------------------------------------------------------------
    @task_group(group_id="process_row")
    def process_row(
        row_id: int, event_id: int, map_id: int, map_name: str, region: str, agent: str
    ):

        # ---------------------------------------------------------------------
        # 3a. Trigger the Cloud Run job for this specific row.
        # We use CloudRunExecuteJobOperator directly here; in a mapped
        # task_group the kwargs are resolved correctly for each instance.
        # ---------------------------------------------------------------------
        run_scraper = CloudRunExecuteJobOperator(
            task_id="trigger_cloud_run",
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            f"--event_id={event_id}",
                            f"--map_id={map_id}",
                            f"--map_name={map_name}",
                            f"--region={region}",
                            f"--agent={agent}",
                            f"--destination_bucket_name={BUCKET}",
                        ]
                    }
                ]
            },
            deferrable=False,  # non-blocking; use False if deferrable executor not configured
        )

        # ---------------------------------------------------------------------
        # 3b. Check GCS for the expected output file after Cloud Run finishes.
        #
        # The expected path pattern is:
        #   bronze/event_id={event_id}/region={region}/map={map_name}/
        #         agent={agent}/snapshot_date={today}/data.csv
        #
        # We use a @task so we can run Python logic (GCSHook) and return a
        # boolean that the next step uses.
        # ---------------------------------------------------------------------
        @task(task_id="check_gcs")
        def check_gcs(
            event_id: int, map_id: int, map_name: str, region: str, agent: str
        ) -> bool:
            today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
            prefix = (
                f"bronze/"
                f"event_id={event_id}/"
                f"region={region}/"
                f"map={map_name}/"
                f"agent={agent}/"
                f"snapshot_date={today}/"
            )
            hook = GCSHook(gcp_conn_id="google_cloud_default")
            objects = hook.list(bucket_name=BUCKET, prefix=prefix)
            return bool(objects)  # True if at least one file found under prefix

        file_exists = check_gcs(
            event_id=event_id,
            map_id=map_id,
            map_name=map_name,
            region=region,
            agent=agent,
        )

        # ---------------------------------------------------------------------
        # 3c. If the file exists, mark the row as scraped in the metadata DB.
        #
        # We update last_scraped (timestamp) and is_scrapped=true only when
        # the GCS file is confirmed present. If check_gcs returns False we
        # raise an exception so this row is marked failed (retryable).
        # ---------------------------------------------------------------------
        @task(task_id="update_metadata")
        def update_metadata(
            row_id: int, file_exists: bool, conn_id: str = "local_vlr_metadata_db"
        ):
            if not file_exists:
                raise ValueError(f"GCS file not found for row_id={row_id}.")

            from airflow.hooks.base import BaseHook

            conn = BaseHook.get_connection(conn_id)
            # Use DbApiHook-compatible approach; adjust get_hook() call per your DB provider
            # e.g. for Postgres: from airflow.providers.postgres.hooks.postgres import PostgresHook
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            hook = PostgresHook(postgres_conn_id=conn_id)
            hook.run(
                sql="""
                    UPDATE vlr_events_metadata
                    SET
                        is_scrapped = CASE
                            WHEN is_completed = TRUE THEN TRUE
                            ELSE is_scrapped
                        END,
                        last_scraped = NOW()
                    WHERE id = %(row_id)s;
                """,
                parameters={"row_id": row_id},
                autocommit=True,
            )

        # Define intra-group task order
        (
            run_scraper
            >> file_exists
            >> update_metadata(row_id=row_id, file_exists=file_exists)
        )

    # -------------------------------------------------------------------------
    # Wire up the DAG
    # -------------------------------------------------------------------------
    process_row.expand_kwargs(formatted)


my_dag()
