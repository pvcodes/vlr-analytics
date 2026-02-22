#!/bin/bash
set -e

PROJECT_ID="vct-analytics"
REGION="asia-south1"
STATE_BUCKET="${PROJECT_ID}-tfstate"

echo "Creating Terraform state bucket..."
gcloud storage buckets create gs://${STATE_BUCKET} \
  --project=${PROJECT_ID} \
  --location=${REGION} \
  --uniform-bucket-level-access

echo "Enabling versioning on state bucket..."
gcloud storage buckets update gs://${STATE_BUCKET} \
  --versioning

echo "Done. State bucket: gs://${STATE_BUCKET}"