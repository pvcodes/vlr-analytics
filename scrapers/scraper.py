import time
from pathlib import Path

from scrapers.stats import vlr_stats
from utils.constants import (
    VLR_MAPS_DICT,
    VCT_STATS_FIELDS,
    DATASET_PATH,
    ENVIRONMENT,
    GCP_VCT_BRONZE_DL,
)
from utils.vct_logging import logger
from utils.helpers import get_jobs_configs, mark_event_as_scraped, write_csv


def stats_scrapper() -> bool:
    BRONZE_DATASET_PATH = (
        Path(DATASET_PATH) / "bronze" if ENVIRONMENT != "PRODUCTION" else Path()
    )

    try:
        x_start = time.perf_counter()
        logger.info(f"ETL started at {time.strftime('%X')}")

        today = time.strftime("%Y-%m-%d")

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

                start = time.perf_counter()
                try:
                    result = vlr_stats(
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
                        bucket_name=(
                            GCP_VCT_BRONZE_DL if ENVIRONMENT == "PRODUCTION" else None
                        ),
                    )
                    logger.info(
                        f"OK | event={event_id} region={region} "
                        f"map={VLR_MAPS_DICT[map_id]} agent={agent} "
                        f"rows={len(result)} time={time.perf_counter() - start:.2f}s"
                    )

                except Exception:
                    logger.exception(
                        f"FAIL | event={event_id} region={region} "
                        f"map={VLR_MAPS_DICT[map_id]} agent={agent}"
                    )

            mark_event_as_scraped(event_id=event_id)

        logger.info(
            f"ETL finished at {time.strftime('%X')}, "
            f"time taken: {time.perf_counter() - x_start:.2f}s"
        )
        return True

    except Exception:
        logger.exception("Scraping failed")
        return False
