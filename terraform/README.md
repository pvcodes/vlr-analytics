# VCT Analytics Infrastructure

Terraform configuration for provisioning the VCT analytics ingestion and orchestration infrastructure on Google Cloud Platform.

This repository defines the infrastructure required to support:

* Bronze data ingestion via Cloud Run Jobs
* Metadata persistence via Cloud SQL (PostgreSQL)
* Lakehouse-style storage using partitioned GCS buckets
* Workflow orchestration using Cloud Composer (Apache Airflow)
* Container artifact management via Artifact Registry
* Event-driven notifications via Pub/Sub
* Serverless Spark processing via Dataproc Serverless
* Remote Terraform state management

---

## Architecture

Infrastructure modules must be deployed in the following order due to inter-module dependencies:

| Module                 | Description                                       |
| ---------------------- | ------------------------------------------------- |
| `00-bootstrap`         | Creates GCS bucket for Terraform remote state    |
| `01-artifact-registry` | Docker Artifact Registry for containers           |
| `02-cloud-sql`         | PostgreSQL instance for metadata                 |
| `03-gcs`              | GCS buckets for Bronze/Silver/Gold layers        |
| `04-cloud-run-job`    | Batch ingestion job for VLR player stats         |
| `04-pub-sub`          | Pub/Sub topics for event notifications           |
| `05-airflow-composer` | Managed Airflow environment for orchestration    |
| `06-dataproc`         | Dataproc Serverless for Spark jobs               |

---

## Prerequisites

* Google Cloud Platform project
* `gcloud` CLI authenticated
* Terraform >= 1.0

Ensure the following APIs are enabled:

* Cloud Run
* Cloud SQL Admin
* Artifact Registry
* Cloud Composer
* Cloud Storage
* Pub/Sub
* Dataproc
* IAM

---

## Deployment

Modules must be deployed sequentially (`00 → 06`).
All Terraform state is stored remotely after bootstrap.

---

### 00 - Bootstrap

Initializes remote Terraform state storage.

```bash
cd 00-bootstrap
terraform init
terraform apply
```

---

### 01 - Artifact Registry

Provisions container registry for ingestion workloads.

```bash
cd 01-artifact-registry
terraform init
terraform apply
```

Verify repository exists:

```bash
gcloud artifacts repositories list --location=<GCP_REGION>
```

Build and push container image:

```bash
gcloud builds submit --tag <GCP_REGION>-docker.pkg.dev/<GCP_PROJECT_ID>/<GCP_ARTIFACT_REPO>/<IMAGE>
```

---

### 02 - Cloud SQL

Creates PostgreSQL instance for metadata tracking.

```bash
cd 02-cloud-sql
terraform init
terraform apply
```

Retrieve instance IP:

```bash
gcloud sql instances describe <db_instance_name> \
  --format="value(ipAddresses.ipAddress)"
```

---

### 03 - Data Lake (GCS)

Creates GCS buckets for analytical storage layers.

```bash
cd 03-gcs
terraform init
terraform apply
```

Buckets created:
* Bronze layer (raw scraped data)
* Silver layer (cleaned/transformed data)
* Gold layer (analytics-ready)

---

### 04 - Cloud Run Job

Deploys the batch ingestion container to scrape VLR player statistics.

```bash
cd 04-cloud-run-job
terraform init
terraform apply
```

Execute job manually:

```bash
gcloud run jobs execute <job_name> \
  --region <GCP_REGION> \
  --args="--event_id=1" \
  --args="--region=eu" \
  --args="--agent=sage" \
  --args="--map_id=1" \
  --args="--map_name=bind" \
  --args="--destination_bucket_name=<bucket_name>"
```

---

### 04 - Pub/Sub

Provisions Pub/Sub topics and subscriptions for event-driven workflows.

```bash
cd 04-pub-sub
terraform init
terraform apply
```

Resources created:
* Completion topic for job status notifications
* Main subscription with dead-letter queue
* Dead-letter topic and subscription

Service account binding grants Cloud Run permission to publish to the topic.

---

### 05 - Airflow Composer

Creates managed Apache Airflow environment for workflow orchestration.

```bash
cd 05-airflow-composer
terraform init
terraform apply
```

If Composer Service Agent permission error occurs:

```bash
gcloud projects add-iam-policy-binding <GCP_PROJECT_ID> \
  --member="serviceAccount:service-<GCP_PROJECT_NUM>@cloudcomposer-accounts.iam.gserviceaccount.com" \
  --role="roles/composer.ServiceAgentV2Ext"
```

Re-run:

```bash
terraform apply
```

---

### 06 - Dataproc Serverless

Provisions Dataproc Serverless runtime for Spark-based transformations (Bronze→Silver).

```bash
cd 06-dataproc
terraform init
terraform apply
```

Service account: `vlr-silver-dp@<project_id>.iam.gserviceaccount.com`

Run Spark job:

```bash
gcloud dataproc batches submit pyspark \
  --region=asia-south1 \
  --project=<project_id> \
  --service-account=vlr-silver-dp@<project_id>.iam.gserviceaccount.com \
  --version=2.2 \
  gs://<bucket>/vlr-silver-transform/main.py \
  -- \
  --base_path gs://<bucket>/bronze \
  --silver_path gs://<bucket>/silver \
  --snapshot_date 2026-02-26
```

---

## Service Accounts

| Account | Purpose |
|---------|---------|
| vct-pipeline-sa | Airflow orchestration |
| vct-cloud-build-sa | CI/CD builds |
| vct-workload-sa | Cloud Run execution |
| vlr-silver-dp-sa | Dataproc Serverless for Silver transformation |
| vct-composer-sa | Cloud Composer |

---

## Next Steps

After deploying all modules:

1. Build and push scraper container: `cd functions/vlr-stats-scrapper && ./build.sh`
2. Build and push silver transform container (if using custom image)
3. Configure Airflow DAGs in Cloud Composer
4. Schedule scraper jobs to populate Bronze layer
5. Schedule Spark jobs to transform Bronze → Silver
