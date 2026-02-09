import asyncio
import time
from pathlib import Path

from scrapers_async.stats import vlr_stats
from utils.config import (
    VLR_STATS_EVENT_SERIES_MAX_ID,
    VLR_AGENTS,
    VLR_REGIONS,
    VLR_MAPS_DICT,
    VLR_EVENTS_DICT,
    ASYNCIO_SEMAPHORE_CONCURRENCY,
)

from utils.logging import logger

from utils.utils import write_csv


# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
BRONZE_DATASET_PATH = Path.cwd() / "data" / "bronze" / "vlr"


# -------------------------------------------------------------------
# Worker
# -------------------------------------------------------------------
async def worker(name: str, queue: asyncio.Queue, sem: asyncio.Semaphore):
    while True:
        job = await queue.get()
        if job is None:
            queue.task_done()
            logger.info(f"Worker {name} shutting down")
            break

        event_id, VLR_REGIONS_DICT, map_id, agent, dest_path = job

        start = time.perf_counter()

        try:
            async with sem:
                result = await vlr_stats(
                    event_group_id=event_id,
                    VLR_REGIONS_DICT=VLR_REGIONS_DICT,
                    agent=agent,
                    map_id=map_id,
                )

            write_csv(dest_path, result)

            elapsed = time.perf_counter() - start
            logger.info(
                f"OK | event={event_id} VLR_REGIONS_DICT={VLR_REGIONS_DICT} "
                f"map={VLR_MAPS_DICT[map_id]} agent={agent} "
                f"rows={len(result)} time={elapsed:.2f}s"
            )

        except Exception as e:
            logger.error(
                f"FAIL | event={event_id} VLR_REGIONS_DICT={VLR_REGIONS_DICT} "
                f"map={VLR_MAPS_DICT[map_id]} agent={agent} | {e}",
                exc_info=True,
            )

        finally:
            queue.task_done()


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
async def stats_scrapper():
    try:
        start_time = time.strftime("%X")
        logger.info(f"ETL started at {start_time}")

        queue = asyncio.Queue()
        sem = asyncio.Semaphore(ASYNCIO_SEMAPHORE_CONCURRENCY)

        # Start workers
        workers = [
            asyncio.create_task(worker(f"W{i + 1}", queue, sem))
            for i in range(ASYNCIO_SEMAPHORE_CONCURRENCY)
        ]

        # Produce jobs
        total_jobs = 0
        for event_id in range(1, VLR_STATS_EVENT_SERIES_MAX_ID + 1):
            for region in VLR_REGIONS:
                for map_id in VLR_MAPS_DICT:
                    for agent in VLR_AGENTS:
                        dest_path = (
                            BRONZE_DATASET_PATH
                            / f"event={VLR_EVENTS_DICT[event_id]}"
                            / f"region={region}"
                            / f"map={VLR_MAPS_DICT[map_id]}"
                            / f"agent={agent}.csv"
                        ).resolve()

                        await queue.put((event_id, region, map_id, agent, dest_path))
                        total_jobs += 1

        logger.info(f"Enqueued {total_jobs} scraping jobs")

        # Wait for all jobs to complete
        await queue.join()

        # Stop workers
        for _ in workers:
            await queue.put(None)

        await asyncio.gather(*workers)

        logger.info(f"ETL finished at {time.strftime('%X')}")
        return True
    except Exception as e:
        logger.error(f"Scraping failed - ", e)
        return False
