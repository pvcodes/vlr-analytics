from pathlib import Path
from utils.vct_logging import logger
from utils.constants import (
    GCS_DATALAKE_BUCKET_NAME,
    ENVIRONMENT,
    VLR_BASE_URL,
    VLR_REQUEST_HEADERS,
    SCRAPER_BATCH_SIZE,
    PROXY_USER,
    PROXY_PSWRD,
)

from utils.gcp import upload_blob_to_gcs
from utils.db import get_db_hook

import httpx
import csv
from typing import List, Tuple, Dict, Optional, Literal
from io import StringIO
import datetime
import os


# =========================================================
# WRITE CSV (LOCAL / GCS)
# =========================================================


def write_csv(
    dest_path: Path,
    rows: List[Dict],
    fields: List[str],
    bucket_name: Optional[str] = None,
    dest_service: Literal["local", "gcs"] = "local",
):

    if dest_service == "gcs" and not bucket_name:
        raise ValueError("Bucket name is required to upload to GCS.")

    if dest_service == "gcs":
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

        upload_blob_to_gcs(bucket_name, dest_path, output.getvalue())
        logger.info(f"Written {len(rows)} rows to gs://{bucket_name}/{dest_path}")

    else:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(dest_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

        logger.info(f"Written {len(rows)} rows to {dest_path}")


# =========================================================
# METADATA DB JOB DISCOVERY (ROW LEVEL)
# =========================================================

METADATA_TABLE = "vlr_events_metadata"


def get_jobs_configs() -> List[Tuple[int, str, str, str, str]]:
    """
    Returns:
    [
      (row_id, event_id, region_abbr, map_id, agent)
    ]
    """

    hook = get_db_hook()

    rows = hook.get_records(
        f"""
        SELECT
            id,
            event_id,
            region_abbr,
            map_id,
            agent
        FROM   {METADATA_TABLE}
        WHERE  is_completed = TRUE
          AND  is_scrapped  = FALSE
        ORDER  BY id
        LIMIT  %s;
        """,
        parameters=(SCRAPER_BATCH_SIZE,),
    )

    if not rows:
        logger.info("No pending scrape jobs found.")
        return []

    logger.info(f"Fetched {len(rows)} pending partitions from metadata DB")

    return [
        (
            int(row_id),
            str(event_id),  # API expects string
            str(region),  # already correct (na, eu...)
            str(map_id),  # 🔥 CRITICAL (dict lookup + API param)
            str(agent),
        )
        for row_id, event_id, region, map_id, agent in rows
    ]


# =========================================================
# MARK PARTITION SCRAPED (BY ROW ID)
# =========================================================


def mark_partition_as_scraped_by_id(row_id: int) -> bool:

    hook = get_db_hook()

    today = datetime.datetime.now(datetime.UTC)

    updated = hook.run(
        f"""
        UPDATE {METADATA_TABLE}
        SET    is_scrapped = TRUE,
               last_scraped = %s
        WHERE  id = %s
          AND  is_completed = TRUE
          AND  is_scrapped  = FALSE;
        """,
        parameters=(today, row_id),
    )

    if updated == 0:
        logger.warning(f"Partition not updated for id={row_id}")
        return False

    return True


# =========================================================
# ASYNC VLR CLIENT (PERSISTENT SESSION)
# =========================================================


# pvcodes_Lxfve
def get_proxies():
    PROXIES = []
    for i in range(1, 6):
        proxy = (
            f"http://user-{PROXY_USER}-country-US:{PROXY_PSWRD}@dc.oxylabs.io:800{i}"
        )
        PROXIES.append(proxy)
    return PROXIES


async def create_client(proxy_url: str):
    client = httpx.AsyncClient(
        base_url=VLR_BASE_URL,
        proxy=proxy_url,
        headers=VLR_REQUEST_HEADERS,
        timeout=30.0,
        http2=True,
        follow_redirects=True,
    )

    # Proper bootstrap — let server set cookies
    await client.get("/")
    await client.get("/stats")

    return client
