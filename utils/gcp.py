from google.cloud import storage
from pathlib import Path
import os

from typing import Dict, Any

from utils.vct_logging import logger

import json

# def upload_csvs_to_gcs(
#     local_folder: str, bucket_name: str, destination_prefix: str = ""
# ):
#     client = storage.Client()
#     bucket = client.bucket(bucket_name)

#     local_path = Path(local_folder)
#     for file_path in local_path.rglob("*.csv"):
#         # Preserve folder structure inside bucket
#         relative_path = file_path.relative_to(local_path)
#         blob_path = os.path.join(destination_prefix, str(relative_path))
#         # print(relative_path, blob_path)
#         blob = bucket.blob(blob_path)
#         blob.upload_from_filename(file_path)

#         print(f"Uploaded {file_path} → gs://{bucket_name}/{blob_path}")


# if __name__ == "__main__":
#     upload_csvs_to_gcs(
#         local_folder="data/bronze",
#         bucket_name="vct-bronze-dl",
#     )


def read_json_from_gcs(bucket_name: str, blob_name: str):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        if not blob.exists():
            logger.warning(f"Blob gs://{bucket_name}/{blob_name} does not exist.")
            return {}

        data_string = blob.download_as_text()
        data_dict = json.loads(data_string)

        logger.info(f"Successfully read JSON from gs://{bucket_name}/{blob_name}")

        return data_dict

    except Exception:
        logger.exception(f"Failed to read JSON from gs://{bucket_name}/{blob_name}")
        raise


def read_blob_from_gcs(bucket_name: str, blob_name: str):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        if not blob.exists():
            logger.warning(f"Blob gs://{bucket_name}/{blob_name} does not exist.")
            raise Exception(f"Blob gs://{bucket_name}/{blob_name} does not exist.")

        data_string = blob.download_as_text()

        return data_string

    except Exception:
        logger.exception(f"Failed to fetch blob from gs://{bucket_name}/{blob_name}")
        raise


def write_json_to_gcs(
    bucket_name: str,
    blob_name: str,
    data: Dict[str, Any],
    use_generation_match: bool = False,
) -> None:
    """
    Write JSON data to GCS.

    If use_generation_match=True, prevents overwriting if blob changed
    (basic optimistic concurrency control).
    """

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        json_data = json.dumps(data, indent=4)

        if use_generation_match and blob.exists():
            blob.reload()
            generation = blob.generation

            blob.upload_from_string(
                json_data,
                content_type="application/json",
                if_generation_match=generation,
            )
        else:
            blob.upload_from_string(
                json_data,
                content_type="application/json",
            )

        logger.info(f"Successfully wrote metadata to gs://{bucket_name}/{blob_name}")

    except Exception:
        logger.exception(f"Failed to write JSON to gs://{bucket_name}/{blob_name}")
        raise


from google.cloud import storage


def upload_blob_to_gcs(
    bucket_name: str,
    destination_blob_name: str,
    data: str,
    content_type: str = "text/csv",
) -> None:
    """
    Uploads string data to a GCS blob.

    Args:
        bucket_name (str): GCS bucket name
        destination_blob_name (str): Path inside bucket
        data (str): File content as string
        content_type (str): MIME type
    """

    try:
        logger.info(
            f"[upload_blob_to_gcs] Uploading {destination_blob_name} to {bucket_name}"
        )

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(
            data,
            content_type=content_type,
        )

        logger.info(
            f"[upload_blob_to_gcs] Upload complete: gs://{bucket_name}/{destination_blob_name}"
        )

    except Exception:
        logger.exception(
            f"[upload_blob_to_gcs] Failed to upload {destination_blob_name}"
        )
        raise
