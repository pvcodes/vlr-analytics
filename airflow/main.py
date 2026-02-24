from dotenv import load_dotenv

load_dotenv()

from google.cloud import storage
import argparse
import logging
import os
from pathlib import Path

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def upload_directory_to_gcs(
    bucket: storage.Bucket, local_dir: Path, gcs_prefix: str
) -> int:
    """Upload all .py files in a local directory to GCS. Returns file count."""
    uploaded = 0
    for file_path in local_dir.rglob("*.py"):
        blob_name = f"{gcs_prefix}/{file_path.relative_to(local_dir)}".replace(
            "\\", "/"
        )
        bucket.blob(blob_name).upload_from_filename(str(file_path))
        logger.info(f"Uploaded: {file_path} -> gs://{bucket.name}/{blob_name}")
        uploaded += 1
    return uploaded


def sync_airflow_configs(bucket_name: str, airflow_home: Path) -> None:
    """Sync Airflow DAGs to GCS."""
    bucket = storage.Client().bucket(bucket_name)
    dags_dir = airflow_home / "dags"
    if dags_dir.exists():
        count = upload_directory_to_gcs(bucket, dags_dir, gcs_prefix="dags")
        logger.info(f"Synced {count} DAG file(s) to gs://{bucket_name}/dags/")
    else:
        logger.warning(f"DAGs directory not found: {dags_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Airflow DAGs to GCS")
    parser.add_argument(
        "--airflow_bucket_name",
        required=False,
        default=os.environ.get("GCP_AIRFLOW_BUCKET"),
        help="Target GCS bucket name (or set GCP_AIRFLOW_BUCKET env var)",
    )
    parser.add_argument(
        "--airflow_home",
        default=os.environ.get("AIRFLOW_HOME", Path.cwd()),
        help="Path to AIRFLOW_HOME (default: $AIRFLOW_HOME or cwd)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.airflow_bucket_name:
        raise ValueError(
            "--airflow_bucket_name is required or set GCP_AIRFLOW_BUCKET env var"
        )

    airflow_home = Path(args.airflow_home).expanduser().resolve()
    if not airflow_home.exists():
        raise FileNotFoundError(f"AIRFLOW_HOME does not exist: {airflow_home}")

    logger.info(f"Syncing from AIRFLOW_HOME: {airflow_home}")
    logger.info(f"Target GCS bucket: {args.airflow_bucket_name}")
    sync_airflow_configs(
        bucket_name=args.airflow_bucket_name, airflow_home=airflow_home
    )
    logger.info("Sync complete.")


if __name__ == "__main__":
    main()
