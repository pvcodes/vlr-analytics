import csv
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from google.cloud import storage


BUCKET = "vlr-data-lake"
GCS_PREFIX = "bronze"
METADATA_TABLE = "vlr_events_metadata"


def _gcs_object_path(cfg: dict) -> str:
    return (
        f"{GCS_PREFIX}"
        f"/event_id={cfg['event_id']}"
        f"/region={cfg['region_abbr']}"
        f"/map={cfg['map_name']}"
        f"/agent={cfg['agent']}"
        f"/snapshot_date={cfg['snapshot_date']}"
        f"/data.csv"
    )


def fetch_rows(limit: int = 500):
    conn = psycopg2.connect(
        host="34.47.187.102",
        port=5432,
        user="airflow",
        password="Thunderbolt7!Spark",
        dbname="vlr_events_metadata",
    )

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                id,
                event_id,
                map_id,
                map_name,
                region_abbr,
                agent,
                last_scraped::date as snapshot_date,
                last_scraped
            FROM {METADATA_TABLE}
            WHERE
                is_completed = TRUE
            AND is_scrapped = TRUE
            ORDER BY
                last_scraped DESC,
                event_id,
                map_id,
                map_name,
                region_abbr,
                agent
            LIMIT %(limit)s;
            """,
            {"limit": limit},
        )
        rows = cur.fetchall()

    conn.close()
    return rows


def audit(limit=500):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET)

    rows = fetch_rows(limit)

    report = []

    for row in rows:
        path = _gcs_object_path(row)

        blob = bucket.blob(path)
        exists = blob.exists()

        report.append(
            {
                "id": row["id"],
                "event_id": row["event_id"],
                "map_name": row["map_name"],
                "region": row["region_abbr"],
                "agent": row["agent"],
                "snapshot_date": row["snapshot_date"],
                "gcs_path": path,
                "exists_in_gcs": exists,
            }
        )

    return report


def write_report(data):
    filename = f"gcs_audit_{datetime.now().date()}.csv"

    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    print(f"Report written to {filename}")


x = [
    "bronze/event_id=1/region=kr/map=haven/agent=killjoy",
    "bronze/event_id=1/region=kr/map=haven/agent=neon",
    "bronze/event_id=1/region=kr/map=haven/agent=kayo",
    "bronze/event_id=1/region=kr/map=haven/agent=jett",
    "bronze/event_id=1/region=kr/map=haven/agent=iso",
    "bronze/event_id=1/region=kr/map=haven/agent=harbor",
    "bronze/event_id=1/region=kr/map=haven/agent=gekko",
    "bronze/event_id=1/region=kr/map=haven/agent=deadlock",
    "bronze/event_id=1/region=kr/map=haven/agent=cypher",
    "bronze/event_id=1/region=kr/map=haven/agent=fade",
    "bronze/event_id=1/region=kr/map=haven/agent=clove",
    "bronze/event_id=1/region=kr/map=haven/agent=brimstone",
    "bronze/event_id=1/region=kr/map=haven/agent=breach",
    "bronze/event_id=1/region=kr/map=haven/agent=astra",
    "bronze/event_id=1/region=kr/map=haven/agent=chamber",
    "bronze/event_id=1/region=jp/map=haven/agent=yoru",
    "bronze/event_id=1/region=jp/map=haven/agent=waylay",
    "bronze/event_id=1/region=jp/map=haven/agent=vyse",
    "bronze/event_id=1/region=jp/map=haven/agent=veto",
    "bronze/event_id=1/region=jp/map=haven/agent=viper",
    "bronze/event_id=1/region=jp/map=haven/agent=tejo",
    "bronze/event_id=1/region=jp/map=haven/agent=raze",
    "bronze/event_id=1/region=jp/map=haven/agent=neon",
    "bronze/event_id=1/region=jp/map=haven/agent=omen",
    "bronze/event_id=1/region=jp/map=haven/agent=phoenix",
    "bronze/event_id=1/region=jp/map=haven/agent=killjoy",
    "bronze/event_id=1/region=jp/map=haven/agent=kayo",
    "bronze/event_id=1/region=jp/map=haven/agent=jett",
    "bronze/event_id=1/region=jp/map=haven/agent=iso",
    "bronze/event_id=1/region=jp/map=haven/agent=gekko",
    "bronze/event_id=1/region=jp/map=haven/agent=fade",
    "bronze/event_id=1/region=gc/map=haven/agent=yoru",
    "bronze/event_id=1/region=gc/map=haven/agent=veto",
    "bronze/event_id=1/region=jp/map=haven/agent=harbor",
]

from collections import defaultdict


def list_partition_contents(prefix: str, bucket):
    """
    Recursively list folders and files under a Bronze logical partition
    """
    blobs = bucket.list_blobs(prefix=prefix)

    folders = defaultdict(list)

    for blob in blobs:
        path = blob.name

        # bronze/.../agent=killjoy/snapshot_date=YYYY-MM-DD/data.csv
        parts = path.split("/")

        try:
            snapshot = next(p for p in parts if p.startswith("snapshot_date="))
            folders[snapshot].append(path)
        except StopIteration:
            continue

    return folders


def x_audit(prefixes):
    client = storage.Client()
    bucket = client.bucket(BUCKET)

    report = {}

    for prefix in prefixes:
        contents = list_partition_contents(prefix, bucket)
        report[prefix] = dict(contents)

    return report


if __name__ == "__main__":
    report = x_audit(x)

    for partition, snapshots in report.items():
        print(f"\n📂 {partition}")
        for folder, files in snapshots.items():
            print(f"  ├── {folder}")
            for f in files:
                print(f"  │    └── {f}")

# if __name__ == "__main__":
# list all content recurevisly
# data = audit(limit=1000)
# write_report(data)
