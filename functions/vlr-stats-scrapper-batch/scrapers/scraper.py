import asyncio
import time
import random
from pathlib import Path

from scrapers.stats import vlr_stats

from utils.constants import (
    VLR_MAPS_DICT,
    VCT_STATS_FIELDS,
    DATASET_PATH,
    ENVIRONMENT,
    GCS_DATALAKE_BUCKET_NAME,
)

from utils.vct_logging import logger
from utils.helpers import (
    get_jobs_configs,
    mark_partition_as_scraped_by_id,
    write_csv,
    get_proxies,
    create_client,
)


# =========================================================
# RETRY WRAPPER
# =========================================================
async def fetch_with_retry(client, event_id, region, agent, map_id, max_retries=5):
    for attempt in range(max_retries):
        try:
            return await vlr_stats(
                client=client,
                event_group_id=event_id,
                region=region,
                agent=agent,
                map_id=map_id,
            )
        except Exception as e:
            wait = (2**attempt) + random.uniform(0.5, 1.5)
            logger.warning(
                f"Retry {attempt+1}/{max_retries} | Sleeping {wait:.2f}s | {e}"
            )
            await asyncio.sleep(wait)

    return None


# =========================================================
# WORKER (1 PER PROXY)
# =========================================================
async def worker(name: str, queue: asyncio.Queue, client):

    # Warm up session
    try:
        await client.get("/")
    except Exception as e:
        logger.warning(f"{name} warmup failed: {e}")

    while True:
        job = await queue.get()

        try:
            if job is None:
                logger.info(f"{name} shutting down")
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

            # Per-IP pacing
            await asyncio.sleep(random.uniform(2.0, 5.0))

            result = await fetch_with_retry(
                client=client,
                event_id=event_id,
                region=region,
                agent=agent,
                map_id=map_id,
            )

            if result:
                write_csv(
                    dest_path=(
                        str(dest_path) if ENVIRONMENT == "PRODUCTION" else dest_path
                    ),
                    rows=result,
                    fields=VCT_STATS_FIELDS,
                    dest_service="gcs" if ENVIRONMENT == "PRODUCTION" else "local",
                    bucket_name=(
                        GCS_DATALAKE_BUCKET_NAME
                        if ENVIRONMENT == "PRODUCTION"
                        else None
                    ),
                )
            else:
                logger.warning(f"Skipping writing 0 rows → {dest_path}")

            mark_partition_as_scraped_by_id(row_id=row_id)

            elapsed = time.perf_counter() - start

            logger.info(
                f"{name} OK | id={row_id} event={event_id} "
                f"map={map_name} agent={agent} "
                f"rows={len(result)} time={elapsed:.2f}s"
            )

        except asyncio.CancelledError:
            raise  # important: don't swallow cancellation

        except Exception as e:
            logger.error(f"{name} FAIL | {e}", exc_info=True)

        finally:
            # ✅ EXACTLY ONE task_done per get()
            queue.task_done()


# =========================================================
# MAIN SCRAPER
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
        PROXIES = get_proxies()
        # Create one client per proxy
        clients = [await create_client(p) for p in PROXIES]

        # One worker per proxy
        workers = [
            asyncio.create_task(worker(f"W{i+1}", queue, clients[i]))
            for i in range(len(PROXIES))
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

        # Shutdown workers
        for _ in workers:
            await queue.put(None)

        await asyncio.gather(*workers)

        # Close clients
        await asyncio.gather(*[c.aclose() for c in clients])

        elapsed = time.perf_counter() - start_time
        logger.info(
            f"ETL finished at {time.strftime('%X')} | time taken: {elapsed:.2f}s"
        )

        return True

    except Exception as e:
        logger.exception(f"Scraping failed - {e}")
        return False
