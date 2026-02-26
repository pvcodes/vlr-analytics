import asyncio
import time
from pathlib import Path

from scrapers.stats import vlr_stats

from utils.constants import (
    VLR_MAPS_DICT,
    ASYNCIO_SEMAPHORE_CONCURRENCY,
    VCT_STATS_FIELDS,
    DATASET_PATH,
    ENVIRONMENT,
    GCP_VCT_BRONZE_DL,
)

from utils.vct_logging import logger
from utils.helpers import (
    get_jobs_configs,
    mark_partition_as_scraped_by_id,
    write_csv,
    get_vlr_client,
)


# =========================================================
# WORKER
# =========================================================


async def worker(
    name: str,
    queue: asyncio.Queue,
    sem: asyncio.Semaphore,
    client,
):
    while True:

        job = await queue.get()

        if job is None:
            queue.task_done()
            logger.info(f"Worker {name} shutting down")
            break

        (
            row_id,
            event_id,
            region,
            map_id,
            map_name,
            agent,
            dest_path,
        ) = job

        start = time.perf_counter()

        try:
            async with sem:
                result = await vlr_stats(
                    client=client,
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

            mark_partition_as_scraped_by_id(row_id=row_id)

            elapsed = time.perf_counter() - start

            logger.info(
                f"OK | id={row_id} event={event_id} region={region} "
                f"map={map_name} agent={agent} "
                f"rows={len(result)} time={elapsed:.2f}s"
            )

        except Exception as e:
            logger.error(
                f"FAIL | id={row_id} event={event_id} region={region} "
                f"map={map_name} agent={agent} | {e}",
                exc_info=True,
            )

        finally:
            queue.task_done()


# =========================================================
# MAIN SCRAPPER
# =========================================================


async def stats_scrapper():

    BRONZE_DATASET_PATH = (
        Path(DATASET_PATH) / "bronze" if ENVIRONMENT != "PRODUCTION" else Path("bronze")
    )

    try:
        start_time = time.perf_counter()
        logger.info(f"ETL started at {time.strftime('%X')}")

        today = time.strftime("%Y-%m-%d")

        queue = asyncio.Queue()
        sem = asyncio.Semaphore(ASYNCIO_SEMAPHORE_CONCURRENCY)

        clients = [await get_vlr_client() for _ in range(ASYNCIO_SEMAPHORE_CONCURRENCY)]

        workers = [
            asyncio.create_task(worker(f"W{i+1}", queue, sem, clients[i]))
            for i in range(ASYNCIO_SEMAPHORE_CONCURRENCY)
        ]

        # =========================================================
        # PRODUCE JOBS
        # =========================================================

        jobs = get_jobs_configs()

        logger.info(f"Enqueued {len(jobs)} scraping jobs")

        for row_id, event_id, region, map_id, agent in jobs:

            map_name = VLR_MAPS_DICT.get(map_id)

            if not map_name:
                logger.warning(f"Unknown map_id={map_id} for row_id={row_id}")
                continue

            dest_path = (
                BRONZE_DATASET_PATH
                / f"event_id={event_id}"
                / f"region={region}"
                / f"map={map_name}"
                / f"agent={agent}"
                / f"snapshot_date={today}"
                / "data.csv"
            )

            await queue.put(
                (
                    row_id,
                    event_id,
                    region,
                    map_id,
                    map_name,
                    agent,
                    dest_path,
                )
            )

        await queue.join()

        for _ in workers:
            await queue.put(None)

        await asyncio.gather(*workers)
        await asyncio.gather(*[c.aclose() for c in clients])

        elapsed = time.perf_counter() - start_time
        logger.info(
            f"ETL finished at {time.strftime('%X')}, " f"time taken: {elapsed:.2f}s"
        )

        return True

    except Exception as e:
        logger.exception(f"Scraping failed - {e}")
        return False
