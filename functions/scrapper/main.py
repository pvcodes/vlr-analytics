import argparse
import csv
from datetime import datetime
from io import StringIO
from typing import List, Dict, Optional

from google.cloud import storage

from utils.scrape import vlr_stats
from utils.logger import logger
from utils.constants import VCT_STATS_FIELDS, GCS_BRONZE_BUCKET_NAME, ENVIRONMENT


def parse_args():
    parser = argparse.ArgumentParser(description="Scrape VLR stats and upload to GCS")

    parser.add_argument("--event_id", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--agent", required=True)
    parser.add_argument("--map_id", required=True)
    parser.add_argument("--map_name", required=True)

    parser.add_argument("--min_rounds", type=int, default=0)
    parser.add_argument("--min_rating", type=float, default=0)
    parser.add_argument("--timespan", default="all")
    parser.add_argument("--destination_bucket_name", default=GCS_BRONZE_BUCKET_NAME)

    return parser.parse_args()


def validate_required_args(args):
    for field in ["event_id", "region", "agent", "map_id", "map_name"]:
        if not getattr(args, field):
            raise ValueError(f"Missing required argument: {field}")


def build_blob_path(event_id, region, map_name, agent, snapshot_date) -> str:
    return (
        f"event_id={event_id}/"
        f"region={region}/"
        f"map={map_name}/"
        f"agent={agent}/"
        f"snapshot_date={snapshot_date}/"
        f"data.csv"
    )


def write_csv(
    blob_path: str,
    rows: List[Dict],
    fields: List[str],
    bucket_name: Optional[str] = None,
    empty_ok: bool = False,
):
    if not rows:
        if not empty_ok:
            raise ValueError(f"Attempted to write empty rows to {blob_path}")
        logger.warning(f"No rows returned, skipping write: {blob_path}")
        return

    if not bucket_name:
        raise ValueError("Bucket name is required.")

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)

    if ENVIRONMENT == "PRODUCTION":
        upload_blob_to_gcs(
            bucket_name=bucket_name,
            destination_blob_name=blob_path,
            data=output.getvalue(),
        )
        logger.info(f"Uploaded {len(rows)} rows → gs://{bucket_name}/{blob_path}")
    else:
        from pathlib import Path

        fpath = Path.cwd() / "data" / "bronze" / blob_path
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_text(output.getvalue())
        logger.info(f"Wrote {len(rows)} rows → {fpath}")


def upload_blob_to_gcs(
    bucket_name: str,
    destination_blob_name: str,
    data: str,
    content_type: str = "text/csv",
) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(data, content_type=content_type)
    logger.info(f"Upload complete: gs://{bucket_name}/{destination_blob_name}")


def main():
    args = parse_args()
    validate_required_args(args)

    today = datetime.now().strftime("%Y-%m-%d")
    blob_path = build_blob_path(
        event_id=args.event_id,
        region=args.region,
        map_name=args.map_name,
        agent=args.agent,
        snapshot_date=today,
    )

    logger.info(
        f"Scraping event={args.event_id} region={args.region} "
        f"agent={args.agent} map={args.map_name} timespan={args.timespan}"
    )

    result = vlr_stats(
        event_group_id=args.event_id,
        region=args.region,
        agent=args.agent,
        map_id=args.map_id,
        min_rounds=args.min_rounds,
        min_rating=args.min_rating,
        timespan=args.timespan,
    )

    logger.info(f"Scrape returned {len(result)} rows")

    write_csv(
        blob_path=blob_path,
        rows=result,
        fields=VCT_STATS_FIELDS,
        bucket_name=args.destination_bucket_name,
        empty_ok=True,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        raise
