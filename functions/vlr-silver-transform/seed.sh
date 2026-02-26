#!/usr/bin/env bash
set -E

if [ -f ".env" ]; then
  set -a
  source .env
  set +a
fi

# ── Config ────────────────────────────────────────────────────────────────────
GCS_BRONZE_PATH="gs://${GCS_DATALAKE_BUCKET_NAME}/bronze"
LOCAL_BRONZE_PATH="data/bronze"

# ── Helpers ───────────────────────────────────────────────────────────────────
log()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
err()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2; }

check_deps() {
  if ! command -v gsutil &> /dev/null; then
    err "gsutil not found. Install the Google Cloud SDK: https://cloud.google.com/sdk/docs/install"
    exit 1
  fi
}

check_env() {
  local missing=0
  for var in GCS_PROJECT_ID GCS_DATALAKE_BUCKET_NAME GCS_REGION; do
    if [ -z "${!var}" ]; then
      err "Required env var \$$var is not set."
      missing=1
    fi
  done
  [ $missing -eq 1 ] && exit 1
}

# ── Main ──────────────────────────────────────────────────────────────────────
main() {
  check_deps
  check_env

  log "Environment  : ${ENVIRONMENT:-unknown}"
  log "Project      : ${GCS_PROJECT_ID}"
  log "Bucket       : ${GCS_DATALAKE_BUCKET_NAME}"
  log "Region       : ${GCS_REGION}"
  log "Source       : ${GCS_BRONZE_PATH}"
  log "Destination  : ${LOCAL_BRONZE_PATH}"
  echo ""

  # Create local directory structure
  mkdir -p "${LOCAL_BRONZE_PATH}"

  # Copy from GCS bronze → local data/bronze
  # Structure: bronze/event_id={}/region={}/map={}/agent={}/snapshot_date={}/data.csv
  log "Syncing bronze data from GCS..."
  gsutil -m cp -r \
    "${GCS_BRONZE_PATH}/*" \
    "${LOCAL_BRONZE_PATH}/"

  if [ $? -eq 0 ]; then
    log "Sync complete."
    log "Files written to: ${LOCAL_BRONZE_PATH}"
    echo ""
    log "Directory structure:"
    find "${LOCAL_BRONZE_PATH}" -name "*.csv" | head -20
  else
    err "gsutil cp failed. Check your GCS credentials and bucket name."
    exit 1
  fi
}

main "$@"