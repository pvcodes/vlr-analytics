"""
vlr_stats_scraper_dispatcher.py
─────────────────────────────────────────────────────────────────────────────
Dispatches un-scraped, completed VLR events to Cloud Run every 15 minutes.

Double-dispatch is prevented by:
  • max_active_runs=1  — only one DAG run active at a time.
  • FOR UPDATE SKIP LOCKED — if a future retry somehow overlaps, rows already
    locked by the active transaction are skipped rather than double-claimed.

No schema changes required.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from functools import cache

from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from google.cloud import run_v2

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────
GCP_PROJECT_ID = "vlr-analytics"
GCP_REGION = "asia-south1"
CLOUD_RUN_JOB = "vlr-stats-scraper"
POSTGRES_CONN = "vlr_metadata_postgres"
TABLE = "vlr_events_metadata"
BATCH_SIZE = 200
CLOUD_RUN_SUBMIT_POOL = "cloud_run_submit_pool"
CLOUD_RUN_OP_TIMEOUT_S = 60


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
@cache
def _jobs_client() -> run_v2.JobsClient:
    """Module-level singleton — reused across expand() calls in the same worker."""
    return run_v2.JobsClient()


def _job_path() -> str:
    return f"projects/{GCP_PROJECT_ID}/locations/{GCP_REGION}/jobs/{CLOUD_RUN_JOB}"


# ─────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────
@dag(
    dag_id="gud_vlr_stats_scraper_dispatcher",
    # schedule="0 * * * *",
    schedule="*/30 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=2,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
    },
    tags=["vlr", "dispatcher"],
    doc_md="""
## VLR Stats Scraper — Dispatcher

Runs every **30 minutes**. Fetches up to 200 rows where
`is_completed = TRUE AND is_scrapped = FALSE` and submits one Cloud Run job
execution per row.

**Double-dispatch protection**
`max_active_runs=1` ensures only one run is active at a time.
`FOR UPDATE SKIP LOCKED` guards against any edge-case overlap during retries.

**Pools**
`cloud_run_submit_pool` — set slot count in the Airflow UI to match your
Cloud Run API quota.
    """,
)
def dispatcher() -> None:

    # ──────────────────────────────────────────
    # TASK 1 — Fetch rows
    # ──────────────────────────────────────────
    @task
    def fetch_configs(ds: str | None = None) -> list[dict]:
        """
        Reads up to BATCH_SIZE rows ready to scrape.
        FOR UPDATE SKIP LOCKED prevents any concurrent transaction (e.g. a
        retry run) from picking up the same rows simultaneously.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, event_id, map_id, map_name, region_abbr, agent
                    FROM   {TABLE}
                    WHERE  is_completed = TRUE
                      AND  is_scrapped  = FALSE
                    ORDER  BY id
                    LIMIT  %(limit)s
                    FOR UPDATE SKIP LOCKED
                    """,
                    {"limit": BATCH_SIZE},
                )
                rows = cur.fetchall()

        if not rows:
            raise AirflowSkipException("Nothing to scrape — skipping run.")

        log.info("Fetched %d rows for dispatch.", len(rows))

        return [
            {
                "row_id": r[0],
                "event_id": r[1],
                "map_id": r[2],
                "map_name": r[3],
                "region": r[4],
                "agent": r[5],
                "snapshot_date": ds,
            }
            for r in rows
        ]

    # ──────────────────────────────────────────
    # TASK 2 — Submit one Cloud Run job per row
    # ──────────────────────────────────────────
    @task(
        pool=CLOUD_RUN_SUBMIT_POOL,
        retries=3,
        retry_delay=timedelta(seconds=10),
        retry_exponential_backoff=True,
    )
    def submit_cloud_run(cfg: dict) -> None:
        """
        Submits a single Cloud Run job execution and waits for the control
        plane to accept it. Any API rejection raises and triggers a retry.
        Job *completion* is tracked via Pub/Sub in the completion DAG.
        """
        overrides = run_v2.RunJobRequest.Overrides(
            container_overrides=[
                run_v2.RunJobRequest.Overrides.ContainerOverride(
                    args=[
                        f"--row_id={cfg['row_id']}",
                        f"--event_id={cfg['event_id']}",
                        f"--map_id={cfg['map_id']}",
                        f"--map_name={cfg['map_name']}",
                        f"--region={cfg['region']}",
                        f"--agent={cfg['agent']}",
                        f"--snapshot_date={cfg['snapshot_date']}",
                    ]
                )
            ]
        )

        log.info(
            "Submitting Cloud Run job — row_id=%s event_id=%s map_id=%s",
            cfg["row_id"],
            cfg["event_id"],
            cfg["map_id"],
        )

        operation = _jobs_client().run_job(
            run_v2.RunJobRequest(name=_job_path(), overrides=overrides)
        )

        try:
            operation.result(timeout=CLOUD_RUN_OP_TIMEOUT_S)
        except Exception as exc:
            raise RuntimeError(
                f"Cloud Run submission failed for row_id={cfg['row_id']}: {exc}"
            ) from exc

        log.info("Cloud Run execution accepted — row_id=%s.", cfg["row_id"])

    # ──────────────────────────────────────────
    # WIRING
    # ──────────────────────────────────────────
    cfgs = fetch_configs()
    submit_cloud_run.expand(cfg=cfgs)


dispatcher()
