from datetime import datetime

from airflow.sdk import dag, task, Asset, AssetAlias, Metadata, Param
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecuteJobOperator,
)

PROJECT_ID = "vlr-analytics"
REGION = "asia-south1"
JOB_NAME = "vlr-stats-scraper"

# Dynamic Bronze Partition Alias
BRONZE_PARTITION = AssetAlias("vlr_bronze_partition")


@dag(
    dag_id="vlr_stats_scrapper_bronze_layer",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bronze"],
    params={
        "event_id": Param("", type="string"),
        "region": Param("", type="string"),
        "map_id": Param("", type="string"),
        "map_name": Param("", type="string"),
        "agent": Param("", type="string"),
        "destination_bucket_name": Param("", type="string"),
    },
)
def bronze_ingestion():

    # 1️⃣ Run Cloud Run Scraper Job
    run_scraper = CloudRunExecuteJobOperator(
        task_id="run_scraper",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        overrides={
            "container_overrides": [
                {
                    "args": [
                        "--event_id",
                        "{{ params.event_id }}",
                        "--region",
                        "{{ params.region }}",
                        "--agent",
                        "{{ params.agent }}",
                        "--map_id",
                        "{{ params.map_id }}",
                        "--map_name",
                        "{{ params.map_name }}",
                        "--destination_bucket_name",
                        "{{ params.destination_bucket_name }}",
                    ]
                }
            ]
        },
    )

    # 2️⃣ Check Cloud Run Execution Status
    @task
    def check_scrape_success(execution):

        try:

            print(execution)
            state = execution["status"]["conditions"][-1]["state"]
        except Exception:
            raise ValueError("Unable to read Cloud Run execution status")

        if state != "SUCCEEDED":
            raise ValueError(f"Cloud Run job failed with state={state}")

        return True

    check = check_scrape_success(run_scraper.output)

    # 3️⃣ Emit Bronze Asset ONLY IF SUCCESS
    @task(outlets=[BRONZE_PARTITION])
    def emit_bronze_asset(event_id, region, map_name, agent, *, outlet_events=None):

        bronze_uri = (
            f"gs://vlr-data-lake/bronze/"
            f"event_id={event_id}/"
            f"region={region}/"
            f"map={map_name}/"
            f"agent={agent}/"
        )

        outlet_events[BRONZE_PARTITION].add(
            Metadata(
                asset=Asset(bronze_uri),
                extra={
                    "event_id": event_id,
                    "region": region,
                    "map": map_name,
                    "agent": agent,
                    "status": "SUCCESS",
                },
            )
        )

    emit = emit_bronze_asset(
        "{{ params.event_id }}",
        "{{ params.region }}",
        "{{ params.map_name }}",
        "{{ params.agent }}",
    )

    # DAG Flow
    run_scraper >> check >> emit


bronze_ingestion()
