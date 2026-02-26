# VLR Analytics Infrastructure

Terraform configuration for provisioning the Valorant Events Stats analytics ingestion and orchestration infrastructure on Google Cloud Platform.

This repository defines the infrastructure required to support:

* Bronze data ingestion via Cloud Run Jobs
* Metadata persistence via Cloud SQL (PostgreSQL)
* Lakehouse-style storage using partitioned GCS buckets
* Workflow orchestration using Cloud Composer (Apache Airflow)
* Container artifact management via Artifact Registry
* Remote Terraform state management

---

## Architecture

Infrastructure modules must be deployed in the following order due to inter-module dependencies:

| Module                 | Description                                       |
| ---------------------- | ------------------------------------------------- |
| `00-bootstrap`         | Creates GCS bucket for Terraform remote state     |
| `01-artifact-registry` | Docker Artifact Registry for ingestion containers |
| `02-cloud-sql`         | PostgreSQL instance for ingestion metadata        |
| `03-datalake-gcs`      | GCS buckets for Bronze/Silver/Gold layers         |
| `04-cloud-run-job`     | Batch ingestion job for VLR player stats          |
| `05-airflow-composer`  | Managed Airflow environment for orchestration     |

Each module provisions resources consumed by downstream ingestion workflows.

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
* IAM

---

## Deployment

Modules must be deployed sequentially (`00 → 05`).
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

Build and push ingestion container image:

```bash
gcloud builds submit --tag <GCP_REGION>-docker.pkg.dev/<GCP_PROJECT_ID>/<GCP_ARTIFACT_REPO>/<IMAGE>
```

If permission errors occur during image push:

```bash
gcloud projects add-iam-policy-binding <GCP_PROJECT_ID> \
  --member="serviceAccount:<GCP_PROJECT_NUM>-compute@developer.gserviceaccount.com" \
  --role="roles/storage.admin"

gcloud projects add-iam-policy-binding <GCP_PROJECT_ID> \
  --member="serviceAccount:<GCP_PROJECT_NUM>-compute@developer.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"
```

Verify pushed image:

```bash
gcloud artifacts docker images list <GCP_REGION>-docker.pkg.dev/<GCP_PROJECT_ID>/<GCP_ARTIFACT_REPO>
```

---

### 02 - Cloud SQL

Creates PostgreSQL instance used for ingestion metadata tracking.

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
cd 03-datalake-gcs
terraform init
terraform apply
```

Verify bucket creation:

```bash
gsutil ls gs://<datalake_bucket_name>
```

---

### 04 - Cloud Run Job

Deploys the batch ingestion container used to scrape and persist VLR player statistics to Bronze storage.

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
  --args="--destination_bucket_name=<datalake_bucket_name>"
```

Job executions overwrite existing Bronze partitions for identical argument sets.

---

### 05 - Airflow Composer

Creates managed Apache Airflow environment used to orchestrate ingestion workflows and trigger Cloud Run Jobs.

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

### 06 - Dataproc Server

<DESCRIPTION>

```bash
cd 06-dataproc
terraform init
terraform apply
```

> email : `dataproc_serverless_sa_email = "vlr-silver-dp@vlr-analytics.iam.gserviceaccount.com"`
---

<!-- All infrastructure resources provisioned here are required for execution of Bronze ingestion DAGs and downstream analytical processing. -->
