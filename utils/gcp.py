from google.cloud import storage

from utils.vct_logging import logger


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
            f"[GCS] Uploading {destination_blob_name} to {bucket_name}"
        )

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(
            data,
            content_type=content_type,
        )

        logger.info(
            f"[GCS] Upload complete: gs://{bucket_name}/{destination_blob_name}"
        )

    except Exception:
        logger.exception(
            f"[GCS] Failed to upload {destination_blob_name}"
        )
        raise
