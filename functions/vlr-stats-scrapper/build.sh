set -E

if [ -f ".env" ]; then
  set -a
  source .env
  set +a
fi


gcloud builds submit --tag "${GCS_REGION}-docker.pkg.dev/${GCS_PROJECT_ID}/${GCS_ARTIFACT_REPO}/${GCS_IMAGE_NAME}"

gcloud run jobs update ${GCR_JOB_NAME} --image="${GCS_REGION}-docker.pkg.dev/${GCS_PROJECT_ID}/${GCS_ARTIFACT_REPO}/${GCS_IMAGE_NAME}" --region=${GCS_REGION}