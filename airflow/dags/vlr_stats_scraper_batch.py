"""
dags/vlr_stats_scraper_batch.py
--------------------------------
Batch DAG for historical VLR stats ingestion.

FLOW:
  1. fetch_pending_events     — query PostgreSQL for events not yet scraped
  2. trigger_scraper_jobs     — fan-out: one Cloud Run Job per event (parallel)
  3. wait_for_all_jobs        — poll each job until SUCCEEDED / FAILED
  4. mark_events_ingested     — update PostgreSQL status for succeeded jobs
  5. publish_completion_msg   — single Pub/Sub message → triggers Silver/Gold DAG

DESIGN DECISIONS:
  - Dynamic task mapping (expand) for parallel Cloud Run jobs — one task per event.
    Airflow 2.3+ feature. Each task is independent so one failure doesn't block others.
  - Pub/Sub fires only once after ALL jobs complete, not per-event.
    The downstream Silver/Gold DAG does a full Silver run over all new data.
  - Failed events are logged and marked in PostgreSQL but don't fail the whole DAG —
    partial ingestion is better than no ingestion for large backfills.

REQUIREMENTS:
  Airflow providers:
    apache-airflow-providers-google>=10.0.0

  Airflow Variables (set in Airflow UI → Admin → Variables):
    vlr_pg_conn_id         — Airflow connection ID for PostgreSQL metadata store
    vlr_project_id         — GCP project ID
    vlr_region             — GCP region for Cloud Run (e.g. us-central1)
    vlr_cloud_run_job_name — Cloud Run Job name (e.g. vlr-stats-scraper-batch)
    vlr_pubsub_topic       — Pub/Sub topic ID (e.g. vlr-ingestion-complete)

  PostgreSQL events table expected schema:
    CREATE TABLE events (
        event_id      INTEGER PRIMARY KEY,
        event_name    TEXT,
        status        TEXT,   -- 'pending' | 'ingested' | 'failed'
        ingested_at   TIMESTAMP
    );
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.cloud_run import CloudRunHook
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config — pulled from Airflow Variables so nothing is hardcoded in the DAG
# ---------------------------------------------------------------------------

PG_CONN_ID = Variable.get("vlr_pg_conn_id", default_var="vlr_postgres")
PROJECT_ID = Variable.get("vlr_project_id")
REGION = Variable.get("vlr_region", default_var="us-central1")
CLOUD_RUN_JOB_NAME = Variable.get(
    "vlr_cloud_run_job_name", default_var="vlr-stats-scraper-batch"
)
PUBSUB_TOPIC = Variable.get("vlr_pubsub_topic", default_var="vlr-ingestion-complete")

# How long to wait for a single Cloud Run Job execution before timing out
JOB_TIMEOUT_SECONDS = 60 * 10  # 10 minutes per event — adjust to your scraper speed

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),  # full DAG timeout
}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------


@dag(
    dag_id="vlr_stats_scraper_batch",
    description="Batch historical ingestion — scrapes all pending VLR events via Cloud Run, then triggers Silver/Gold via Pub/Sub",
    schedule_interval=None,  # manual trigger only — this is a backfill DAG
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["vlr", "bronze", "batch", "scraping"],
    # Prevent concurrent runs — running two batch DAGs simultaneously
    # would cause duplicate scrapes and PostgreSQL race conditions
    max_active_runs=1,
)
def vlr_stats_scraper_batch():

    # -----------------------------------------------------------------------
    # Task 1: Fetch pending events from PostgreSQL
    # -----------------------------------------------------------------------

    @task()
    def fetch_pending_events() -> list[dict]:
        """
        Query PostgreSQL for all events with status = 'pending'.

        Returns a list of dicts, one per event:
          [{"event_id": 1, "event_name": "VCT 2024 Americas"}, ...]

        WHY return dicts not just IDs:
          Dynamic task mapping passes the full dict downstream so each
          Cloud Run task has all the context it needs without extra DB queries.
        """
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

        rows = hook.get_records(
            """
            SELECT event_id, event_name
            FROM   events
            WHERE  status = 'pending'
            ORDER  BY event_id
        """
        )

        if not rows:
            logger.info("No pending events found — nothing to scrape.")
            return []

        events = [{"event_id": row[0], "event_name": row[1]} for row in rows]
        logger.info(
            f"Found {len(events)} pending events: {[e['event_id'] for e in events]}"
        )
        return events

    # -----------------------------------------------------------------------
    # Task 2: Trigger one Cloud Run Job execution per event (parallel fan-out)
    # -----------------------------------------------------------------------

    @task()
    def trigger_scraper_job(event: dict) -> dict:
        """
        Trigger a Cloud Run Job execution for a single event.

        Uses dynamic task mapping — Airflow calls this once per item
        in the list returned by fetch_pending_events, all in parallel.

        Returns execution metadata so the next task can poll status.

        WHY CloudRunHook not CloudRunJobOperator:
          The Operator blocks until the job completes, which means tasks
          would run sequentially. Using the Hook lets us fire all jobs
          at once and poll separately — true parallel fan-out.
        """
        event_id = event["event_id"]
        event_name = event["event_name"]

        logger.info(f"Triggering Cloud Run Job for event_id={event_id} ({event_name})")

        hook = CloudRunHook(gcp_conn_id="google_cloud_default", region=REGION)

        # Fire the job — Cloud Run reads event context from PostgreSQL itself
        # No args needed — scraper knows what to do from the metadata DB
        execution = hook.create_job_execution(
            job_name=CLOUD_RUN_JOB_NAME,
            project_id=PROJECT_ID,
        )

        execution_name = (
            execution.name
        )  # e.g. projects/proj/locations/region/jobs/job/executions/abc
        logger.info(f"event_id={event_id} → execution started: {execution_name}")

        return {
            "event_id": event_id,
            "event_name": event_name,
            "execution_name": execution_name,
        }

    # -----------------------------------------------------------------------
    # Task 3: Poll each execution until done
    # -----------------------------------------------------------------------

    @task()
    def wait_for_job(execution_meta: dict) -> dict:
        """
        Poll a Cloud Run Job execution until it reaches a terminal state.
        Returns enriched dict with final status for downstream tasks.

        Terminal states:
          SUCCEEDED — scrape completed successfully
          FAILED    — scrape failed (logged, event marked failed, DAG continues)
          CANCELLED — manually cancelled

        WHY poll here instead of using a Sensor:
          With 100 events, 100 separate Sensor tasks would flood the Airflow
          scheduler. Polling inside a @task keeps the task graph clean and
          uses one worker slot per event.
        """
        import time

        event_id = execution_meta["event_id"]
        execution_name = execution_meta["execution_name"]

        hook = CloudRunHook(gcp_conn_id="google_cloud_default", region=REGION)
        start = time.time()
        interval = 20  # poll every 20 seconds

        logger.info(f"Polling event_id={event_id} execution: {execution_name}")

        while True:
            execution = hook.get_job_execution(
                job_name=CLOUD_RUN_JOB_NAME,
                execution_id=execution_name.split("/")[-1],
                project_id=PROJECT_ID,
            )

            state = execution.reconciling  # True = still running
            conditions = {c.type_: c.state for c in execution.conditions}

            # Completed states
            if "Completed" in conditions:
                completed_state = conditions["Completed"]
                if str(completed_state) == "1":  # True = success
                    logger.info(f"event_id={event_id} SUCCEEDED")
                    return {**execution_meta, "status": "ingested"}
                else:
                    logger.error(
                        f"event_id={event_id} FAILED — execution: {execution_name}"
                    )
                    return {**execution_meta, "status": "failed"}

            elapsed = time.time() - start
            if elapsed > JOB_TIMEOUT_SECONDS:
                logger.error(f"event_id={event_id} TIMED OUT after {elapsed:.0f}s")
                return {**execution_meta, "status": "failed"}

            logger.info(
                f"event_id={event_id} still running ({elapsed:.0f}s elapsed)..."
            )
            time.sleep(interval)

    # -----------------------------------------------------------------------
    # Task 4: Mark events as ingested/failed in PostgreSQL
    # -----------------------------------------------------------------------

    @task()
    def mark_events_ingested(results: list[dict]) -> dict:
        """
        Bulk update PostgreSQL event statuses based on Cloud Run outcomes.

        Accepts the full list of result dicts from wait_for_job.
        Splits into succeeded / failed and updates in two bulk queries.

        Returns summary counts for the Pub/Sub message.
        """
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

        succeeded = [r["event_id"] for r in results if r["status"] == "ingested"]
        failed = [r["event_id"] for r in results if r["status"] == "failed"]

        if succeeded:
            hook.run(
                """
                UPDATE events
                SET    status = 'ingested',
                       ingested_at = NOW()
                WHERE  event_id = ANY(%s)
                """,
                parameters=(succeeded,),
            )
            logger.info(f"Marked {len(succeeded)} events as ingested: {succeeded}")

        if failed:
            hook.run(
                """
                UPDATE events
                SET    status = 'failed'
                WHERE  event_id = ANY(%s)
                """,
                parameters=(failed,),
            )
            logger.warning(f"Marked {len(failed)} events as failed: {failed}")

        return {
            "succeeded_count": len(succeeded),
            "failed_count": len(failed),
            "succeeded_ids": succeeded,
        }

    # -----------------------------------------------------------------------
    # Task 5: Publish Pub/Sub message → triggers Silver/Gold DAG
    # -----------------------------------------------------------------------

    @task()
    def publish_completion_message(summary: dict) -> None:
        """
        Publish a single Pub/Sub message after all Cloud Run jobs complete.
        The downstream Silver/Gold DAG is triggered by a Pub/Sub subscription
        listening on this topic.

        Message payload includes succeeded event IDs so the Silver DAG
        can run targeted transforms if needed (currently it runs full Silver).

        WHY one message for all events (not one per event):
          Silver/Gold runs as a single Spark job over ALL new data.
          Publishing per-event would trigger N Spark jobs unnecessarily.
          One message = one Silver run = one Gold run.
        """
        if summary["succeeded_count"] == 0:
            logger.warning("No events succeeded — skipping Pub/Sub publish.")
            return

        hook = PubSubHook(gcp_conn_id="google_cloud_default")

        message = {
            "pipeline": "vlr_batch_scraper",
            "trigger": "batch_complete",
            "succeeded_count": summary["succeeded_count"],
            "failed_count": summary["failed_count"],
            "event_ids": summary["succeeded_ids"],
            "triggered_at": datetime.utcnow().isoformat(),
        }

        hook.publish(
            project_id=PROJECT_ID,
            topic=PUBSUB_TOPIC,
            messages=[
                {
                    "data": json.dumps(message).encode("utf-8"),
                    "attributes": {"source": "vlr_batch_dag"},
                }
            ],
        )

        logger.info(
            f"Pub/Sub published to {PUBSUB_TOPIC} — "
            f"{summary['succeeded_count']} events ingested, "
            f"{summary['failed_count']} failed."
        )

    # -----------------------------------------------------------------------
    # Wire the DAG
    # -----------------------------------------------------------------------

    # Step 1 — get pending events
    pending_events = fetch_pending_events()

    # Step 2 — fan-out: one Cloud Run job per event, all in parallel
    # .expand() is Airflow's dynamic task mapping — generates N tasks at runtime
    executions = trigger_scraper_job.expand(event=pending_events)

    # Step 3 — poll each execution until terminal state
    results = wait_for_job.expand(execution_meta=executions)

    # Step 4 — bulk update PostgreSQL
    # results is a list of dicts — passed as a whole list, not expanded
    summary = mark_events_ingested(results)

    # Step 5 — notify downstream pipeline
    publish_completion_message(summary)


vlr_stats_scraper_batch()
