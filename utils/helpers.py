from pathlib import Path
from utils.vct_logging import logger
from utils.constants import (
    GCP_VCT_BRONZE_DL,
    ENVIRONMENT,
    VLR_BASE_URL,
    VLR_REQUEST_HEADERS,
)

from utils.gcp import upload_blob_to_gcs
from utils.db import get_db_hook

import httpx
import csv
from typing import List, Tuple, Dict, Optional, Literal
from io import StringIO
from datetime import datetime
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
    empty_ok: bool = False,
):

    if not rows:
        if not empty_ok:
            raise ValueError(f"Attempted to write empty rows to {dest_path}. Aborting.")
        logger.warning(f"Empty result → skipping write: {dest_path}")
        return

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
BATCH_SIZE = int(os.environ.get("SCRAPER_BATCH_SIZE", 1000))


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
        parameters=(BATCH_SIZE,),
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

    today = datetime.today().strftime("%Y-%m-%d")

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


async def get_vlr_client() -> httpx.AsyncClient:

    client = httpx.AsyncClient(
        base_url=VLR_BASE_URL,
        headers=VLR_REQUEST_HEADERS,
        follow_redirects=True,
        timeout=httpx.Timeout(30.0),
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
    )

    # bootstrap PHPSESSID
    await client.get("/stats")

    client.cookies.set("abok", "1")

    return client
