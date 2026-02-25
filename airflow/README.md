
# VLR Analytics – Airflow 3.1.x Orchestration Layer

This directory contains the Apache Airflow setup used to orchestrate Bronze layer ingestion jobs in the VLR Analytics pipeline.

Airflow is responsible for:

* Scheduling ingestion workflows
* Triggering Cloud Run Jobs for VLR player stats scraping
* Managing runtime argument injection
* Syncing DAG definitions to Google Cloud Storage (GCS) for remote execution environments

---

## Project Structure

```
airflow/
├── dags/
│   └── vlr_stats_scrapper_bronze_layer.py   # Bronze ingestion DAG
├── plugins/                                 # Custom operators/hooks (if any)
├── main.py                                  # DAG sync to GCS script
├── start.sh                                 # Local standalone Airflow launcher
├── pyproject.toml                           # Project dependencies
└── uv.lock                                  # Locked dependency versions
```

---

## Local Development Setup

### Install Dependencies

This project uses `uv` for dependency management.

```bash
uv sync
```

---

### Environment Variables

Copy the example environment file:

```bash
cp .env.example .env
```

Update with your values:

```
GCP_AIRFLOW_BUCKET=asia-south2-vlr-airflow-com-fdd59011-bucket
AIRFLOW_HOME=/absolute/path/to/airflow
```

---

## Running Airflow Locally

Use the provided startup script:

```bash
./start.sh
```

This launches Airflow in **standalone mode** with the following runtime fixes:

* macOS fork safety override (required for multiprocessing)
* Native gRPC DNS resolver (required for Google Cloud SDK)
* Disabled proxy resolution conflicts
* Python fault handler enabled for debugging scheduler crashes

Airflow UI will be available at:

```
http://localhost:8080
```

---

## DAG Deployment (GCS Sync)

To deploy DAGs to a remote Composer or shared GCS-backed environment:

```bash
python main.py
```

This uploads all `.py` DAG files from:

```
$AIRFLOW_HOME/dags/
```

to:

```
gs://<GCP_AIRFLOW_BUCKET>/dags/
```

Uploads are deterministic and overwrite existing DAG definitions.

---

## Bronze Ingestion Workflow

The DAG:

```
vlr_stats_scrapper_bronze_layer.py
```

is responsible for triggering the:

> VLR Player Stats Bronze Ingestion Cloud Run Job

using:

* `CloudRunExecuteJobOperator`
* Runtime container argument overrides
* Deterministic Bronze partition keys

Each execution generates or overwrites a partition in:

```
gs://vlr-data-lake/bronze/
```

based on

```
bronze/
  event_id=<event_id>/
    region=<region>/
      map=<map_name>/
        agent=<agent>/
          snapshot_date=<YYYY-MM-DD>/
            data.csv
```

---

## Dependencies

* `apache-airflow`
* `apache-airflow-providers-google`
* `apache-airflow-providers-postgres`
* `google-cloud-storage`

---

## Notes

* Airflow standalone mode is intended for local orchestration only.
* Production scheduling is expected to run via Cloud Composer or equivalent managed Airflow environments.
* DAG sync script must be executed after DAG updates to reflect changes in remote environments.

---

## License

MIT
