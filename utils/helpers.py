from pathlib import Path
from utils.vct_logging import logger
from utils.constants import (
    GCP_VCT_BRONZE_DL,
    ENVIRONMENT,
    EVENTS_STATUS_CSV_FIELDS,
    EVENTS_STATUS_CSV_FILENAME,
)
from utils.gcp import (
    read_blob_from_gcs,
    upload_blob_to_gcs,
)
import csv
from typing import List, Tuple, Dict, Optional, Literal
from collections import defaultdict
from itertools import product
from datetime import datetime
from io import StringIO
import os

from utils.constants import (
    VLR_AGENTS,
    VLR_MAPS_DICT,
    VLR_EVENTS_DICT,
    VLR_REGIONS_DICT,
)


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
            raise ValueError(
                f"Attempted to write empty rows to {dest_path}. Aborting to avoid data loss."
            )
        logger.warning(f"Empty result → skipping write: {dest_path}")
        return

    if dest_service == "gcs" and not bucket_name:
        err = "Bucket name is required to upload to GCS."
        logger.exception(err)
        raise ValueError(err)

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


def read_csv(path: Path, fields: List[str]) -> Tuple[Dict, List[Dict]]:
    with open(path, "r") as f:
        reader = csv.DictReader(f, fieldnames=fields)
        header = next(reader, None)
        return header, list(reader)


def load_events_status() -> List[Dict]:
    if ENVIRONMENT == "PRODUCTION":
        logger.info("Reading events_status.csv from GCS")
        data_string = read_blob_from_gcs(GCP_VCT_BRONZE_DL, EVENTS_STATUS_CSV_FILENAME)
        reader = csv.DictReader(
            data_string.splitlines(),
            fieldnames=EVENTS_STATUS_CSV_FIELDS,
        )
        events_status = list(reader)
    else:
        bronze_path = Path(os.environ.get("DATASET_PATH")) / "bronze"
        filepath = bronze_path / EVENTS_STATUS_CSV_FILENAME
        _, events_status = read_csv(path=filepath, fields=EVENTS_STATUS_CSV_FIELDS)

    return events_status


def save_events_status(events_status: List[Dict]):
    if not events_status:
        raise ValueError(
            "Attempted to save empty events_status. Aborting to avoid data loss."
        )

    if ENVIRONMENT == "PRODUCTION":
        logger.info("Writing updated events_status.csv to GCS")
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=EVENTS_STATUS_CSV_FIELDS)
        writer.writerows(events_status)
        upload_blob_to_gcs(
            GCP_VCT_BRONZE_DL, EVENTS_STATUS_CSV_FILENAME, output.getvalue()
        )
    else:
        bronze_path = Path(os.environ.get("DATASET_PATH")) / "bronze"
        filepath = bronze_path / EVENTS_STATUS_CSV_FILENAME
        write_csv(
            dest_path=filepath, rows=events_status, fields=EVENTS_STATUS_CSV_FIELDS
        )


def get_jobs_configs() -> List[Dict[str, List[Tuple[str, str, str]]]]:
    try:
        events_status = load_events_status()
        logger.info("Events Status loaded successfully")

    except FileNotFoundError:
        logger.exception("events_status.csv not found.")
        raise

    except Exception:
        logger.exception("Unexpected error while loading events_status.csv")
        raise

    # completed partitions
    completed = {
        (job["event_id"], job["region_id"], job["map_id"], job["agent_id"])
        for job in events_status
        if job["is_completed"] == "true" and job["is_scraped"] == "true"
    }

    # all possible partitions
    all_jobs = set(
        product(
            VLR_EVENTS_DICT.keys(),
            VLR_REGIONS_DICT.keys(),
            VLR_MAPS_DICT.keys(),
            VLR_AGENTS,
        )
    )

    # pending partitions
    pending = all_jobs - completed

    # group by event_id
    grouped = defaultdict(list)
    for event_id, region, map_id, agent in pending:
        grouped[event_id].append((region, map_id, agent))

    pending_jobs = [
        {"event_id": event, "jobs": jobs} for event, jobs in grouped.items()
    ]

    logger.info(f"Total pending events: {len(pending_jobs)}")

    return pending_jobs


def mark_event_as_scraped(event_id: str) -> bool:
    if not event_id:
        err = "event_id is required"
        logger.exception(err)
        raise ValueError(err)

    today = datetime.today().strftime("%Y-%m-%d")

    events_status = load_events_status()

    updated = False
    for row in events_status:
        if row["event_id"].strip() == str(event_id).strip():
            row["is_scraped"] = "true"
            row["last_scraped"] = today
            updated = True
            break

    if not updated:
        err = f"{event_id} not found in events_status.csv"
        logger.exception(err)
        raise Exception(err)

    save_events_status(events_status)
    logger.info(f"{event_id} marked as scraped on {today}")
    return True
