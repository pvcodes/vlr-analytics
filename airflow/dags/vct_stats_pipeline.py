from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task
from datetime import datetime, timedelta
import asyncio

from scrapers_async.stats import vlr_stats
from utils.helpers import get_jobs_configs, write_csv
from utils.constants import VCT_STATS_FIELDS, GCP_VCT_BRONZE_DL, VLR_MAPS_DICT
from utils.vct_logging import logger


from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@task
def get_events_to_scrape():
    """Returns list of event configs to scrape"""
    configs = get_jobs_configs()
    return configs


@task
def scrape_single_event(event_config: dict):
    """Scrapes all jobs for a single event"""
    import time
    from pathlib import Path

    event_id = event_config["event_id"]
    jobs = event_config["jobs"]
    today = time.strftime("%Y-%m-%d")

    logger.info(f"Starting event_id={event_id} with {len(jobs)} jobs")

    async def run_event_jobs():
        tasks = []
        for region, map_id, agent in jobs:
            dest_path = (
                f"event_id={event_id}/"
                f"region={region}/"
                f"map={VLR_MAPS_DICT[map_id]}/"
                f"agent={agent}/"
                f"snapshot_date={today}/"
                "data.csv"
            )

            async def scrape_job(r, m, a, path):
                try:
                    result = await vlr_stats(
                        event_group_id=event_id,
                        region=r,
                        agent=a,
                        map_id=m,
                    )

                    write_csv(
                        dest_path=path,
                        rows=result,
                        fields=VCT_STATS_FIELDS,
                        empty_ok=True,
                        dest_service="gcs",
                        bucket_name=GCP_VCT_BRONZE_DL,
                    )

                    logger.info(
                        f"OK | event={event_id} region={r} "
                        f"map={VLR_MAPS_DICT[m]} agent={a} rows={len(result)}"
                    )
                    return True

                except Exception as e:
                    logger.error(
                        f"FAIL | event={event_id} region={r} "
                        f"map={VLR_MAPS_DICT[m]} agent={a} | {e}"
                    )
                    return False

            tasks.append(scrape_job(region, map_id, agent, dest_path))

        # Run all jobs for this event concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success = sum(1 for r in results if r is True)

        logger.info(f"Event {event_id} complete: {success}/{len(jobs)} successful")
        return {"event_id": event_id, "success": success, "total": len(jobs)}

    result = asyncio.run(run_event_jobs())

    if result["success"] == 0:
        raise Exception(f"All jobs failed for event {event_id}")

    return result


with DAG(
    "vct_stats_pipeline_parallel",
    default_args=default_args,
    description="VCT stats ETL - parallel by event",
    start_date=datetime(2025, 2, 1),
    catchup=False,
    max_active_runs=1,
    tags=["vct", "etl", "parallel"],
) as dag:

    events = get_events_to_scrape()

    # This creates one task per event automatically
    scrape_results = scrape_single_event.expand(event_config=events)

    # Downstream tasks (load to BigQuery, etc) go here
    # They'll wait for ALL scrape tasks to finish
