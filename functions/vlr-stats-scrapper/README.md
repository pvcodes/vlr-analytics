# VLR Player Stats Bronze Ingestion Job

This is a Google **Cloud Run Job container** that scrapes player statistics from [vlr.gg/stats](https://www.vlr.gg/stats) based on the given [query parameters](#command-line-arguments) and saves them to a configured path in Google Cloud Storage (GCS) or locally during development.

>This job is intended to be executed as a **batch ingestion unit** inside the VLR Analytics ingestion pipeline and is typically triggered via Apache Airflow using runtime arguments.

---

## Installation

- Create a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

- Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Usage

### Command Line Arguments

| Argument                    | Required | Description                              |
| --------------------------- | -------- | ---------------------------------------- |
| `--event_id`                | Yes      | Event group ID from VLR.gg               |
| `--region`                  | Yes      | Region filter (e.g., na, eu, ap)         |
| `--agent`                   | Yes      | Agent filter (e.g., jett, raze, omen)    |
| `--map_id`                  | Yes      | Map ID from VLR.gg                       |
| `--map_name`                | Yes      | Map name for Bronze partition path       |
| `--snapshot_date`           | Yes      | Snapshot date (YYYY-MM-DD)               |
| `--min_rounds`              | No       | Minimum rounds played (default: 0)       |
| `--min_rating`              | No       | Minimum rating (default: 0)              |
| `--timespan`                | No       | Time period filter (default: all)        |
| `--destination_bucket_name` | No       | GCS bucket name (default: vlr-data-lake) |

---

### Local Development (Dev Only)

```bash
python main.py \
  --event_id=1 \
  --region=na \
  --agent=jett \
  --map_id=1 \
  --map_name=ascent \
  --snapshot_date=2025-02-25
```

This writes CSV files locally to:

```
data/bronze/
```

---

### Production (Cloud Run Job)

In production, this container is executed via:

- Google Cloud Run Job
- Apache Airflow (`CloudRunExecuteJobOperator`)

Arguments are passed at runtime through container overrides.

Successful execution guarantees that a fully written CSV is uploaded for the requested partition or no object is written.

---

## Output

### CSV Fields (Schema Version: v1)

| Field                        | Description                |
| ---------------------------- | -------------------------- |
| player_id                    | VLR.gg player identifier   |
| player                       | Player name                |
| org                          | Player's organization/team |
| agents                       | Agent played               |
| rounds_played                | Total rounds played        |
| rating                       | Player rating              |
| average_combat_score         | Average Combat Score (ACS) |
| kill_deaths                  | K/D ratio                  |
| kill_assists_survived_traded | KAST percentage            |
| average_damage_per_round     | ADR                        |
| kills_per_round              | KPR                        |
| assists_per_round            | APR                        |
| first_kills_per_round        | FKPR                       |
| first_deaths_per_round       | FDPR                       |
| headshot_percentage          | HS%                        |
| clutch_success_percentage    | Clutch success %           |
| clutches_won_played_ratio    | Clutches won/played        |
| max_kills_in_single_map      | KMAX                       |
| kills                        | Total kills                |
| deaths                       | Total deaths               |
| assists                      | Total assists              |
| first_kills                  | Total first kills          |
| first_deaths                 | Total first deaths         |

Schema is considered stable and backward-compatible changes must be additive.

---

### File Path Structure

All Bronze data is written using deterministic Hive-style partitioning:

```
bronze/
  event_id=<event_id>/
    region=<region>/
      map=<map_name>/
        agent=<agent>/
          snapshot_date=<YYYY-MM-DD>/
            data.csv
```

This partition structure is required for downstream ingestion and external table inference.

Write mode is **overwrite**, making executions idempotent for identical argument sets.

---

## Deployment

### Deploy the Image to GCP Artifact Registry

1. Run Terraform for [artifact registry](../../terraform/01-artifact-registry/)

```bash
terraform init
terraform apply
```

1. Copy the `.env.example` to `.env` and update based on your config:

```bash
cp .env.example .env
```

1. Run the [build.sh](build.sh) to submit the image to Artifact Registry:

```bash
./build.sh
```

---

### Deploy to Cloud Run Job

Deploy the Cloud Run Job manually or via Terraform [here](../../terraform/README.md#04---cloud-run-job).

```bash
gcloud run deploy vlr-stats-scraper \
  --image ${GCS_REGION}-docker.pkg.dev/${GCS_PROJECT_ID}/${GCS_ARTIFACT_REPO}/${GCS_IMAGE_NAME} \
  --region asia-south1
```

---

## Project Structure

```
vlr-stats-scrapper/
  main.py              # Entry point and GCS upload logic
  Dockerfile           # Container definition
  build.sh             # Docker build script
  requirements.txt     # Python dependencies
  utils/
    scrape.py          # VLR.gg scraping logic
    constants.py       # Configuration and field definitions
    logger.py          # Logging setup with Cloud Logging
```

---

## Dependencies

- `google-cloud-logging` - Cloud Logging integration
- `google-cloud-storage` - GCS upload functionality
- `selectolax` - HTML parsing
- `requests` - HTTP requests

---

## License

MIT
