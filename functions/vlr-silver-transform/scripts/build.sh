#!/usr/bin/env bash
set -eEuo pipefail

if [ -f "$PWD/.env" ]; then
  set -a
  source .env
  set +a
fi

: "${GCS_CODE_BUCKET:?not set}"
: "${SILVER_TRANSFORM_PATH:?not set}"
: "${GCS_PROJECT_ID:?not set}"

gcloud storage cp main.py \
  gs://$GCS_CODE_BUCKET/$SILVER_TRANSFORM_PATH/main.py \
  --project=$GCS_PROJECT_ID