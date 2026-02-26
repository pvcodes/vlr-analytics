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
    """Sync Airflow DAGs and Plugins to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    sync_map = {
        "dags": airflow_home / "dags",
        "plugins": airflow_home / "plugins",
    }

    for gcs_prefix, local_path in sync_map.items():
        if local_path.exists():
            count = upload_directory_to_gcs(bucket, local_path, gcs_prefix=gcs_prefix)
            logger.info(f"Synced {count} file(s) to gs://{bucket_name}/{gcs_prefix}/")
        else:
            logger.warning(f"{gcs_prefix} directory not found: {local_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Airflow DAGs & Plugins to GCS")

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
