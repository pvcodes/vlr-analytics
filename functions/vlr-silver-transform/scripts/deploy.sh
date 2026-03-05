#!/usr/bin/env bash
set -eEuo pipefail

if [ -f ".env" ]; then
  set -a
  source .env
  set +a
fi

: "${GCS_CODE_BUCKET:?not set}"
: "${SILVER_TRANSFORM_PATH:?not set}"
: "${GCS_REGION:?not set}"
: "${GCS_PROJECT_ID:?not set}"
: "${GCS_DATALAKE_BUCKET_NAME:?not set}"

SNAPSHOT_DATE=""
PIPELINE_VERSION=""
# GCS_REGION="us-central1"


for arg in "$@"; do
  case $arg in
    --snapshot_date=*)    SNAPSHOT_DATE="${arg#*=}" ;;
    --pipeline_version=*) PIPELINE_VERSION="${arg#*=}" ;;
    *) echo "Unknown argument: $arg"; exit 1 ;;
  esac
done

: "${SNAPSHOT_DATE:?--snapshot_date is required}"

declare -a EXTRA_ARGS=()
if [ -n "$PIPELINE_VERSION" ]; then
  EXTRA_ARGS+=(--pipeline_version="$PIPELINE_VERSION")
fi

gcloud dataproc batches submit pyspark \
  gs://$GCS_CODE_BUCKET/$SILVER_TRANSFORM_PATH/main.py \
  --project=$GCS_PROJECT_ID \
  --region=$GCS_REGION \
  --version=2.2 \
  --properties="\
spark.executor.instances=2,\
spark.executor.cores=4,\
spark.executor.memory=24g,\
spark.executor.memoryOverhead=4g,\
spark.driver.cores=4,\
spark.driver.memory=24g,\
spark.driver.memoryOverhead=4g,\
spark.shuffle.io.maxRetries=10,\
spark.shuffle.io.retryWait=60s,\
spark.sql.shuffle.partitions=8" \
  -- \
  --base_path=gs://$GCS_DATALAKE_BUCKET_NAME/bronze \
  --silver_path=gs://$GCS_DATALAKE_BUCKET_NAME/silver \
  --snapshot_date="$SNAPSHOT_DATE" \
  ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}