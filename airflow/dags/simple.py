from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecuteJobOperator,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import task


PROJECT_ID = "vlr-analytics"
REGION = "asia-south1"
JOB_NAME = "vlr-stats-scraper"


with DAG(
    dag_id="test_cloud_run_job",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
    max_active_tasks=4,
) as dag:

    # -------- Read Metadata from Cloud SQL --------
    read_events_metadata = SQLExecuteQueryOperator(
        task_id="read_events_metadata",
        conn_id="local_vlr_metadata_db",
        sql="""
            SELECT event_id, map_id, map_name, region_abbr, agent
            FROM vlr_events_metadata
            WHERE is_scrapped = false
            LIMIT 10;
        """,
        handler=lambda cursor: cursor.fetchall(),  # REQUIRED in Airflow 3.x
    )

    # -------- Convert Tuple → Dict & Prepare Overrides --------
    @task
    def format_for_cloud_run(rows):
        return [
            {
                "event_id": row[0],
                "map_id": row[1],
                "map_name": row[2],
                "region": row[3],
                "agent": row[4],
                "overrides": {
                    "container_overrides": [
                        {
                            "args": [
                                f"--event_id={row[0]}",
                                f"--map_id={row[1]}",
                                f"--map_name={row[2]}",
                                f"--region={row[3]}",
                                f"--agent={row[4]}",
                                "--destination_bucket_name=vlr-data-lake",
                            ]
                        }
                    ]
                },
            }
            for row in rows
        ]

    formatted = format_for_cloud_run(read_events_metadata.output)

    # -------- Dynamically Trigger Cloud Run Jobs --------
    run_scraper = CloudRunExecuteJobOperator.partial(
        task_id="trigger_vlr_scraper",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
    ).expand(overrides=formatted.map(lambda x: x["overrides"]))

    # -------- Update DB Per Successful Job --------


@task
def update_event_db(meta):
    from airflow.providers.common.sql.hooks.sql import SqlHook

    hook = SqlHook(conn_id="local_vlr_metadata_db")

    hook.run(
        """
        UPDATE vlr_events_metadata
        SET
            last_scrapped = NOW(),
            is_scrapped = CASE
                WHEN is_completed = TRUE THEN TRUE
                ELSE is_scrapped
            END
        WHERE
            event_id = %s
            AND map_id = %s
            AND map_name = %s
            AND region_abbr = %s
            AND agent = %s;
        """,
        parameters=(
            meta["event_id"],
            meta["map_id"],
            meta["map_name"],
            meta["region"],
            meta["agent"],
        ),
    )

    update_task = update_event_db.expand(meta=formatted)

    # -------- Dependencies --------
    read_events_metadata >> formatted >> run_scraper >> update_task
