"""
DAG:     vlr_stats_scraper
Version: 9.0 — per-config independent chains via @task_group
Runtime: Airflow 3.1.x · GCP Composer 3

Flow:
    fetch_configs
        └──→ process_config.expand(config=...)
                 ├── execute_cloud_run   ─┐
                 ├── check_gcs_landed    ←┘─┐
                 └── mark_scraped        ←──┘
             (N independent chains, one per config)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.sdk import (
    dag,
    task,
    task_group,
    get_current_context,
    PokeReturnValue,
)
from airflow.exceptions import AirflowSkipException

log = logging.getLogger(__name__)

# ╔══════════════════════════════════════════════════════════════╗
# ║  CONSTANTS                                                   ║
# ╚══════════════════════════════════════════════════════════════╝
GCP_PROJECT_ID = "vlr-analytics"
GCP_REGION = "asia-south1"
GCP_CONN_ID = "google_cloud_default"
CLOUD_RUN_JOB = "vlr-stats-scraper"
GCS_BUCKET = "vlr-data-lake"
GCS_PREFIX = "bronze"
METADATA_CONN_ID = "vlr_metadata_postgres"
METADATA_TABLE = "vlr_events_metadata"
BATCH_SIZE = 50

CLOUD_RUN_POOL = "cloud_run_pool"  # Admin → Pools → slots = 5
GCS_SENSOR_POOL = "gcs_sensor_pool"  # Admin → Pools → slots = 10
DB_POOL = "postgres_pool"  # Admin → Pools → slots = 5

MAX_PARALLEL_CR = 5
SENSOR_POKE_SEC = 30
SENSOR_TIMEOUT_SEC = 900  # 15 min
CR_TIMEOUT_SEC = 7200  # 2 h

DEFAULT_ARGS = {
    "owner": "pvcodes",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}


# ╔══════════════════════════════════════════════════════════════╗
# ║  HELPERS                                                     ║
# ╚══════════════════════════════════════════════════════════════╝
def _label(config: dict) -> str:
    return (
        f"{config['event_id']}/{config['region']}/"
        f"{config['map_name']}/{config['agent']}"
    )


# ╔══════════════════════════════════════════════════════════════╗
# ║  DAG                                                         ║
# ╚══════════════════════════════════════════════════════════════╝
@dag(
    dag_id="vlr_stats_scraper",
    schedule="0 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    default_args=DEFAULT_ARGS,
    tags=["vlr", "scraping", "cloud-run"],
    doc_md=__doc__,
)
def vlr_stats_scrape_dag():

    # ── 1. FETCH CONFIGS ──────────────────────────────────────
    @task
    def fetch_configs(ds: str = None) -> list[dict]:
        """Query unscraped rows, return one config dict per row."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        rows = PostgresHook(postgres_conn_id=METADATA_CONN_ID).get_records(
            f"""
            SELECT id, event_id, map_id, map_name, region_abbr, agent
            FROM   {METADATA_TABLE}
            WHERE  is_completed = TRUE
              AND  is_scrapped  = FALSE
            ORDER  BY event_id, map_id, map_name, region_abbr, agent
            LIMIT  {BATCH_SIZE};
            """
        )

        if not rows:
            raise AirflowSkipException("No unscraped rows — skipping entire run")

        configs: list[dict] = []
        for r in rows:
            event_id, map_id, map_name = r[1], r[2], r[3]
            region, agent = r[4], r[5]

            configs.append(
                {
                    "row_id": r[0],
                    "event_id": event_id,
                    "map_id": map_id,
                    "map_name": map_name,
                    "region": region,
                    "agent": agent,
                    "gcs_object": (
                        f"{GCS_PREFIX}/event_id={event_id}/region={region}/"
                        f"map={map_name}/agent={agent}/"
                        f"snapshot_date={ds}/data.csv"
                    ),
                    "ds": ds,
                }
            )

        log.info("Fetched %d config(s) to process", len(configs))
        return configs

    # ── 2. PER-CONFIG TASK GROUP ──────────────────────────────
    @task_group
    def process_config(config: dict):
        """Independent 3-step chain for a single config.
        Each mapped instance runs cloud_run → gcs_check → mark_scraped
        without waiting for other configs to finish any stage."""

        # ── 2a. EXECUTE CLOUD RUN ─────────────────────────────
        @task(
            pool=CLOUD_RUN_POOL,
            max_active_tis_per_dagrun=MAX_PARALLEL_CR,
            retries=2,
            retry_delay=timedelta(minutes=3),
        )
        def execute_cloud_run(cfg: dict) -> dict:
            """Trigger Cloud Run job and block until it finishes.
            Idempotent: skips if the GCS file already exists."""
            from google.cloud import run_v2
            from airflow.providers.google.cloud.hooks.gcs import GCSHook

            label = _label(cfg)

            # ── idempotency: skip if file already landed ──
            if GCSHook(gcp_conn_id=GCP_CONN_ID).exists(
                bucket_name=GCS_BUCKET, object_name=cfg["gcs_object"]
            ):
                log.info("[%s] File already in GCS — skipping Cloud Run", label)
                cfg["skipped"] = True
                return cfg

            # ── trigger Cloud Run ──
            overrides = run_v2.RunJobRequest.Overrides(
                container_overrides=[
                    run_v2.RunJobRequest.Overrides.ContainerOverride(
                        args=[
                            f"--event_id={cfg['event_id']}",
                            f"--map_id={cfg['map_id']}",
                            f"--map_name={cfg['map_name']}",
                            f"--region={cfg['region']}",
                            f"--agent={cfg['agent']}",
                            f"--destination_bucket_name={GCS_BUCKET}",
                            f"--snapshot_date={cfg['ds']}",
                        ]
                    )
                ]
            )

            job_path = (
                f"projects/{GCP_PROJECT_ID}"
                f"/locations/{GCP_REGION}"
                f"/jobs/{CLOUD_RUN_JOB}"
            )

            operation = run_v2.JobsClient().run_job(
                run_v2.RunJobRequest(name=job_path, overrides=overrides)
            )
            log.info("[%s] Cloud Run triggered — waiting …", label)

            execution = operation.result(timeout=CR_TIMEOUT_SEC)

            condition = (
                execution.conditions[-1].type_ if execution.conditions else "UNKNOWN"
            )
            log.info("[%s] Cloud Run completed — condition=%s", label, condition)

            cfg["skipped"] = False
            return cfg

        # ── 2b. CHECK GCS LANDING ────────────────────────────
        @task.sensor(
            poke_interval=SENSOR_POKE_SEC,
            timeout=SENSOR_TIMEOUT_SEC,
            mode="poke",
            exponential_backoff=True,
            pool=GCS_SENSOR_POOL,
        )
        def check_gcs_landed(cfg: dict) -> PokeReturnValue:
            """Poke GCS until the expected file exists."""
            from airflow.providers.google.cloud.hooks.gcs import GCSHook

            if cfg.get("skipped"):
                return PokeReturnValue(is_done=True, xcom_value=cfg)

            exists = GCSHook(gcp_conn_id=GCP_CONN_ID).exists(
                bucket_name=GCS_BUCKET, object_name=cfg["gcs_object"]
            )

            if exists:
                log.info(
                    "[row=%s] ✓ GCS confirmed: %s",
                    cfg["row_id"],
                    cfg["gcs_object"],
                )
                return PokeReturnValue(is_done=True, xcom_value=cfg)

            log.info(
                "[row=%s] waiting for: %s",
                cfg["row_id"],
                cfg["gcs_object"],
            )
            return PokeReturnValue(is_done=False)

        # ── 2c. MARK SCRAPED ─────────────────────────────────
        @task(pool=DB_POOL, retries=3)
        def mark_scraped(cfg: dict) -> None:
            """Set is_scrapped=TRUE and last_scraped=NOW()."""
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            PostgresHook(postgres_conn_id=METADATA_CONN_ID).run(
                f"""
                UPDATE {METADATA_TABLE}
                SET    is_scrapped  = TRUE,
                       last_scraped = NOW()
                WHERE  id = %s
                  AND  is_completed = TRUE;
                """,
                parameters=(cfg["row_id"],),
            )
            log.info(
                "[row=%s] ✓ SCRAPED — %s",
                cfg["row_id"],
                _label(cfg),
            )

        # ── chain inside the group ────────────────────────────
        cr_result = execute_cloud_run(config)
        gcs_result = check_gcs_landed(cr_result)
        mark_scraped(gcs_result)

    # ╔══════════════════════════════════════════════════════════╗
    # ║  WIRING                                                  ║
    # ╚══════════════════════════════════════════════════════════╝
    configs = fetch_configs()
    process_config.expand(config=configs)


vlr_stats_scrape_dag()
