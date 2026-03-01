from pathlib import Path
from utils.vct_logging import logger
from utils.constants import (
    VLR_BASE_URL,
    VLR_REQUEST_HEADERS,
    SCRAPER_BATCH_SIZE,
    PROXY_USER,
    PROXY_PSWRD,
    METADATA_TABLE,
)

from utils.gcp import upload_blob_to_gcs
from utils.db import get_db_hook

import httpx
import csv
from typing import List, Tuple, Dict, Optional, Literal
from io import StringIO
import datetime


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
# Fetch data from completed events which are not scrapped and mark them as picked
# =========================================================
def get_jobs_configs() -> List[Tuple[int, str, str, str, str]]:

    hook = get_db_hook()

    query = f"""
        UPDATE {METADATA_TABLE} AS m
        SET    is_picked = TRUE,
               picked_at = NOW()
        FROM (
            SELECT id
            FROM   {METADATA_TABLE}
            WHERE  is_completed = TRUE
              AND  is_scrapped  = FALSE
              AND  is_picked    = FALSE
            ORDER  BY id
            LIMIT  %s
            FOR UPDATE SKIP LOCKED
        ) AS sub
        WHERE m.id = sub.id
        RETURNING
            m.id,
            m.event_id,
            m.region_abbr,
            m.map_id,
            m.agent;
    """

    rows = hook.run(query, (SCRAPER_BATCH_SIZE,), fetch=True)

    if not rows:
        logger.info("No pending scrape jobs found.")
        return []

    logger.info(f"Picked {len(rows)} partitions for scraping")

    return [
        (
            int(row_id),
            str(event_id),
            str(region),
            str(map_id),
            str(agent),
        )
        for row_id, event_id, region, map_id, agent in rows
    ]


def release_partition_by_id(row_id: int) -> bool:

    hook = get_db_hook()

    updated = hook.run(
        f"""
        UPDATE {METADATA_TABLE}
        SET    is_picked = FALSE,
               picked_at = NULL
        WHERE  id = %s
          AND  is_picked = TRUE;
        """,
        parameters=(row_id,),
    )

    return updated == 1


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
               is_picked = FALSE,
               picked_at = NULL,
               last_scraped = %s
        WHERE  id = %s
          AND  is_completed = TRUE
          AND  is_scrapped  = FALSE;
        """,
        parameters=(today, row_id),
    )

    return updated == 1


def get_proxies():
    PROXIES = []
    for i in range(1, 6):
        proxy = f"http://user-{PROXY_USER}-country-US:{PROXY_PSWRD}@dc.oxylabs.io:800{i}"  # specific to oxylabs
        PROXIES.append(proxy)
    return PROXIES


# =========================================================
# ASYNC VLR CLIENT (PERSISTENT SESSION)
# =========================================================
async def create_client(proxy_url: str):
    client = httpx.AsyncClient(
        base_url=VLR_BASE_URL,
        proxy=proxy_url,
        headers=VLR_REQUEST_HEADERS,
        timeout=30.0,
        http2=True,
        follow_redirects=True,
    )

    # Proper bootstrap — to set cookies
    await client.get("/")
    await client.get("/stats")

    return client
