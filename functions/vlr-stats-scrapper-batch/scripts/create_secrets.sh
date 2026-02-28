#!/bin/bash

set -e

ENV_FILE=${1:-$PWD/.env.prod}
PROJECT_ID="vlr-analytics"

if [ ! -f "$ENV_FILE" ]; then
  echo "File $ENV_FILE not found!"
  exit 1
fi

echo "Loading secrets from $ENV_FILE"
echo ""

# Only treat these keys as secrets
SECRET_KEYS=(
  DB_HOST
  DB_USER
  DB_PASSWORD
  PROXY_USER
  PROXY_PSWRD
)

while IFS='=' read -r key value
do
  # Trim whitespace
  key=$(echo "$key" | xargs)
  value=$(echo "$value" | xargs)

  # Skip comments or empty lines
  if [[ -z "$key" || "$key" == \#* ]]; then
    continue
  fi

  # Remove surrounding quotes if present
  value="${value%\"}"
  value="${value#\"}"
  value="${value%\'}"
  value="${value#\'}"

  # Only create defined secret keys
  if [[ " ${SECRET_KEYS[@]} " =~ " ${key} " ]]; then

    echo "Processing secret: $key"

    if gcloud secrets describe "$key" --project="$PROJECT_ID" >/dev/null 2>&1; then
      echo "  ➜ Adding new version"
      echo -n "$value" | gcloud secrets versions add "$key" \
        --data-file=- \
        --project="$PROJECT_ID"
    else
      echo "  ➜ Creating secret"
      echo -n "$value" | gcloud secrets create "$key" \
        --replication-policy="automatic" \
        --data-file=- \
        --project="$PROJECT_ID"
    fi

    echo "Done"
    echo ""
  fi

done < "$ENV_FILE"

echo "Secrets synced successfully!"