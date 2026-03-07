
# VLR Analytics — Airflow Orchestration

Apache Airflow 3.x setup for orchestrating VLR stats scraping and data pipeline.

## Overview

Manages the Bronze layer ingestion workflow:

1. **Dispatcher DAG** — Runs every 30 min, submits Cloud Run jobs for pending events
2. **Completion DAG** — Runs every 5 min, polls Pub/Sub for job completions, updates PostgreSQL
3. **Batch DAG** — Manual trigger for historical backfill of pending events

---

## DAGs

### Dispatcher (`vlr_stats_scraper_dispatcher`)

- **Schedule**: Every 30 minutes
- **Purpose**: Fetches up to 200 un-scraped completed events from PostgreSQL and submits one Cloud Run job per row
- **Protection**: `FOR UPDATE SKIP LOCKED` prevents double-dispatch
- **Output**: Triggers Cloud Run jobs → Pub/Sub completion messages

### Completion (`vlr_stats_scraper_completion`)

- **Schedule**: Every 5 minutes
- **Purpose**: Pulls Pub/Sub messages from `vlr-stats-scraper-completion-sub`, marks successful rows as `is_scrapped = TRUE`
- **Failure handling**: Failed scrapes are not acknowledged → Pub/Sub redelivers

### Batch (`vlr_stats_scraper_batch`)

- **Schedule**: Manual trigger only
- **Purpose**: Historical backfill — fetches all pending events, runs Cloud Run jobs in parallel, publishes completion Pub/Sub

---

## Project Structure

```
airflow/
├── dags/
│   ├── vlr_stats_scraper_dispatcher.py   # 30-min event dispatcher
│   ├── vlr_stats_scraper_completion.py    # Pub/Sub completion handler
│   └── vlr_stats_scraper_batch.py        # Historical backfill
├── main.py                                # DAG sync to GCS
├── start.sh                               # Local Airflow standalone
├── pyproject.toml
└── uv.lock
```

---

## Local Development

### Install Dependencies

```bash
uv sync
```

### Environment Variables

```bash
cp .env.example .env
```

Required variables:

```
GCP_AIRFLOW_BUCKET=asia-south2-vlr-airflow-com-fdd59011-bucket
AIRFLOW_HOME=/absolute/path/to/airflow
```

### Run Airflow Locally

```bash
./start.sh
```

Airflow UI: <http://localhost:8080>

---

## Deployment

### Sync DAGs to GCS

```bash
python main.py
```

Uploads DAGs from `$AIRFLOW_HOME/dags/` to `gs://<GCP_AIRFLOW_BUCKET>/dags/`

### Airflow Variables

Set in Airflow UI → Admin → Variables:

| Variable | Description |
|----------|-------------|
| `vlr_pg_conn_id` | PostgreSQL connection for metadata |
| `vlr_project_id` | GCP project ID |
| `vlr_region` | GCP region for Cloud Run |
| `vlr_cloud_run_job_name` | Cloud Run Job name |
| `vlr_pubsub_topic` | Pub/Sub topic for completion |

---

## Dependencies

- `apache-airflow>=3.1.0`
- `apache-airflow-providers-google`
- `apache-airflow-providers-postgres`
- `apache-airflow-providers-pubsub`
- `google-cloud-run`
- `google-cloud-storage`

---

## Notes

- Standalone mode is for local development only
- Production uses Cloud Composer or GCS-backed remote execution
- Run `python main.py` after any DAG changes to sync to remote
