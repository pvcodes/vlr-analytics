# VCT Analytics Pipeline Workflow

## Overview

This document describes the data pipeline architecture for the VCT (Valorant Champions Tour) Analytics project. The pipeline follows a **Medallion Architecture** (Bronze/Silver/Gold) pattern and runs every 15 days via Cloud Composer (Airflow).

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Cloud Composer (Airflow)                          │
│                         Orchestrates every 15 days                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Step 1: VLR Stats Scrapper                          │
│                          (Cloud Run Job)                                    │
│  Image: asia-south1-docker.pkg.dev/$PROJECT_ID/vct-analytics/              │
│         vlr-stats-scrapper:latest                                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Bronze Layer (GCS)                                │
│                      gs://vct-analytics-bronze                              │
│                                                                             │
│  Path: event_id={}/region={}/map={}/agent={}/snapshot_date={}/data.csv     │
│  Content: Raw scraped player statistics from VLR.GG                         │
│  Retention: 180 days                                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Step 2: Bronze to Silver Transform                     │
│                          (Dataflow - Apache Beam)                           │
│                                                                             │
│  TODO: Implement Dataflow pipeline for transformations                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Silver Layer (GCS)                                │
│                      gs://vct-analytics-silver                              │
│                                                                             │
│  TODO: Create bucket and define schema                                      │
│  Content: Cleaned, validated, and deduplicated data                         │
│           Wide column format with all player stats consolidated             │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Step 3: Silver to Gold Transform                       │
│                          (Dataflow - Apache Beam)                           │
│                                                                             │
│  TODO: Implement Dataflow pipeline for aggregations                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Gold Layer (GCS/BigQuery)                        │
│                       gs://vct-analytics-gold                               │
│                                                                             │
│  TODO: Create bucket and define schema                                      │
│  Content: Aggregated, analytics-ready data                                  │
│           Optimized for downstream consumption (BigQuery, dashboards)       │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Current Implementation (Bronze Layer Only)

### VLR Stats Scrapper (Cloud Run Job)

**Location:** `functions/vlr-stats-scrapper/`

**Purpose:** Scrapes player statistics from VLR.GG for configured events, regions, maps, and agents.

**Cloud Run Job:** `vlr-stats-scrapper`

**Container Image:**
```
asia-south1-docker.pkg.dev/vct-analytics/vct-analytics/vlr-stats-scrapper:latest
```

**Environment Variables:**
| Variable | Description |
|----------|-------------|
| `ENVIRONMENT` | `PRODUCTION` or `LOCAL` |
| `DESTINATION_BUCKET_NAME` | Target GCS bucket (bronze layer) |

**Output Path Format:**
```
gs://vct-analytics-bronze/event_id={event_id}/region={region}/map={map}/agent={agent}/snapshot_date={date}/data.csv
```

**Data Fields (23 columns):**
- `player_id`, `player`, `org`, `agents`
- `rounds_played`, `rating`, `average_combat_score`
- `kill_deaths`, `kill_assists_survived_traded`
- `average_damage_per_round`, `kills_per_round`, `assists_per_round`
- `first_kills_per_round`, `first_deaths_per_round`
- `headshot_percentage`, `clutch_success_percentage`, `clutches_won_played_ratio`
- `max_kills_in_single_map`, `kills`, `deaths`, `assists`
- `first_kills`, `first_deaths`

## Infrastructure

### Service Accounts

| Service Account | Purpose | Key Permissions |
|-----------------|---------|-----------------|
| `vct-pipeline-sa` | Airflow/Composer orchestration | Storage Admin, Run Invoker, Dataflow Admin |
| `vct-cloud-build-sa` | CI/CD image builds | Artifact Registry Writer, Storage Viewer |
| `vct-workload-sa` | Cloud Run job execution (Bronze) | Storage Object Admin, Artifact Registry Reader |
| `vct-dataflow-sa` | Dataflow jobs (Silver/Gold) | Dataflow Worker, Storage Object Admin, BigQuery Data Editor |
| `vct-composer-sa` | Cloud Composer environment | Composer Worker |

### GCS Buckets

| Bucket | Purpose | Status |
|--------|---------|--------|
| `vct-analytics-bronze` | Raw scraped data (180 day retention) | Implemented |
| `vct-analytics-silver` | Cleaned/transformed data | TODO |
| `vct-analytics-gold` | Analytics-ready data | TODO |
| `vct-analytics-tfstate` | Terraform state | Implemented |

### Cloud Run Jobs

| Job | CPU | Memory | Timeout | Retries | Status |
|-----|-----|--------|---------|---------|--------|
| `vlr-stats-scrapper` | 1 | 512Mi | 15 min | 3 | Implemented |

## Deployment

### Prerequisites

1. GCP Project with billing enabled
2. Required APIs enabled (via Terraform)
3. Docker images pushed to Artifact Registry

### Build and Push Images

```bash
# VLR Stats Scrapper
cd functions/vlr-stats-scrapper
docker build -t asia-south1-docker.pkg.dev/vct-analytics/vct-analytics/vlr-stats-scrapper:latest .
docker push asia-south1-docker.pkg.dev/vct-analytics/vct-analytics/vlr-stats-scrapper:latest
```

### Deploy Infrastructure

```bash
cd terraform

# Initialize
terraform init

# Plan
terraform plan

# Apply
terraform apply
```

### Manual Job Execution

```bash
# Run VLR Stats Scrapper
gcloud run jobs execute vlr-stats-scrapper --region asia-south1
```

## Future Work (Silver/Gold Layers)

### Dataflow Pipelines (To Be Implemented)

The Silver and Gold layer transformations will use **Google Cloud Dataflow** (Apache Beam):

#### Bronze to Silver Pipeline
- Data validation and schema enforcement
- Deduplication based on player_id + event + snapshot_date
- Data type conversions (strings to numeric)
- Null handling and default values
- Consolidate into wide column format

#### Silver to Gold Pipeline
- Aggregations (player-level, team-level, event-level)
- Derived metrics calculation
- Historical trend analysis
- Format for BigQuery ingestion

### Infrastructure TODO

1. Add Silver and Gold GCS buckets to `modules/storage/`
2. Create `modules/dataflow/` for Dataflow job templates
3. Enable Dataflow API in `modules/apis/`
4. Create Apache Beam pipeline code in `pipelines/`

## Airflow DAG (To Be Implemented)

The Airflow DAG should:

1. Trigger `vlr-stats-scrapper` Cloud Run job
2. Wait for completion
3. Trigger Bronze-to-Silver Dataflow job
4. Wait for completion
5. Trigger Silver-to-Gold Dataflow job
6. Wait for completion
7. Send notification on success/failure

**Schedule:** Every 15 days (`0 0 */15 * *`)

Example DAG structure:

```python
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'vct-analytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'vct_analytics_pipeline',
    default_args=default_args,
    description='VCT Analytics Data Pipeline',
    schedule_interval='0 0 */15 * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    scrape_vlr_stats = CloudRunExecuteJobOperator(
        task_id='scrape_vlr_stats',
        project_id='vct-analytics',
        region='asia-south1',
        job_name='vlr-stats-scrapper',
    )

    # TODO: Add Dataflow operators for Silver/Gold transformations
    # bronze_to_silver = DataflowStartFlexTemplateOperator(...)
    # silver_to_gold = DataflowStartFlexTemplateOperator(...)

    scrape_vlr_stats  # >> bronze_to_silver >> silver_to_gold
```

## Monitoring

### Cloud Logging

Filter logs by resource type:

```
# Cloud Run Jobs
resource.type="cloud_run_job"
resource.labels.job_name="vlr-stats-scrapper"

# Dataflow (future)
resource.type="dataflow_step"
```

### Alerting (Recommended)

Set up alerts for:
- Job failures
- Job duration exceeding threshold
- Storage quota warnings
