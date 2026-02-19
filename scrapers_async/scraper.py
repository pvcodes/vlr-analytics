import asyncio
import time
from pathlib import Path

from scrapers_async.stats import vlr_stats
from utils.constants import (
    VLR_MAPS_DICT,
    ASYNCIO_SEMAPHORE_CONCURRENCY,
    VCT_STATS_FIELDS,
    DATASET_PATH,
    ENVIRONMENT,
    GCP_VCT_BRONZE_DL,
)

from utils.vct_logging import logger
from utils.helpers import get_jobs_configs, mark_event_as_scraped, write_csv


async def worker(name: str, queue: asyncio.Queue, sem: asyncio.Semaphore):
    while True:
        job = await queue.get()
        if job is None:
            queue.task_done()
            logger.info(f"Worker {name} shutting down")
            break

        event_id, region, map_id, agent, dest_path = job

        start = time.perf_counter()

        try:
            async with sem:
                result = await vlr_stats(
                    event_group_id=event_id,
                    region=region,
                    agent=agent,
                    map_id=map_id,
                )

            write_csv(
                dest_path=(
                    str(dest_path) if ENVIRONMENT == "PRODUCTION" else dest_path
                ),
                rows=result,
                fields=VCT_STATS_FIELDS,
                empty_ok=True,
                dest_service="gcs" if ENVIRONMENT == "PRODUCTION" else "local",
                bucket_name=GCP_VCT_BRONZE_DL if ENVIRONMENT == "PRODUCTION" else None,
            )

            elapsed = time.perf_counter() - start
            logger.info(
                f"OK | event={event_id} region={region} "
                f"map={VLR_MAPS_DICT[map_id]} agent={agent} "
                f"rows={len(result)} time={elapsed:.2f}s"
            )

        except Exception as e:
            logger.error(
                f"FAIL | event={event_id} region={region} "
                f"map={VLR_MAPS_DICT[map_id]} agent={agent} | {e}",
                exc_info=True,
            )

        finally:
            queue.task_done()


async def stats_scrapper():
    BRONZE_DATASET_PATH = (
        Path(DATASET_PATH) / "bronze" if ENVIRONMENT != "PRODUCTION" else Path()
    )

    try:
        start_time = time.perf_counter()
        logger.info(f"ETL started at {time.strftime('%X')}")

        today = time.strftime("%Y-%m-%d")

        queue = asyncio.Queue()
        sem = asyncio.Semaphore(ASYNCIO_SEMAPHORE_CONCURRENCY)

        workers = [
            asyncio.create_task(worker(f"W{i + 1}", queue, sem))
            for i in range(ASYNCIO_SEMAPHORE_CONCURRENCY)
        ]

        # Produce jobs
        jobs_config = get_jobs_configs()
        jobs_config = jobs_config[:1]
        total_jobs = sum(len(configs["jobs"]) for configs in jobs_config)
        logger.info(f"Enqueued {total_jobs} scraping jobs")

        for configs in jobs_config:
            event_id = configs["event_id"]
            for region, map_id, agent in configs["jobs"]:
                dest_path = (
                    BRONZE_DATASET_PATH
                    / f"event_id={event_id}"
                    / f"region={region}"
                    / f"map={VLR_MAPS_DICT[map_id]}"
                    / f"agent={agent}"
                    / f"snapshot_date={today}"
                    / "data.csv"
                )
                await queue.put((event_id, region, map_id, agent, dest_path))

        # Wait for all jobs to complete
        await queue.join()

        # Stop workers
        for _ in workers:
            await queue.put(None)
        await asyncio.gather(*workers)

        # Mark events as scraped
        for configs in jobs_config:
            _ = mark_event_as_scraped(event_id=configs["event_id"])

        elapsed = time.perf_counter() - start_time
        logger.info(
            f"ETL finished at {time.strftime('%X')}, time taken: {elapsed:.2f}s"
        )

        return True

    except Exception as e:
        logger.exception(f"Scraping failed - {e}")
        return False
