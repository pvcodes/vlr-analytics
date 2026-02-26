"""
vlr_stats_scraper_completion.py
─────────────────────────────────────────────────────────────────────────────
Polls Pub/Sub every 5 minutes for scraper completion messages, marks
successful rows in Postgres, then acknowledges the messages.

Failure handling (no schema changes):
  • Successful rows  → is_scrapped = TRUE, last_scraped = NOW(), then acked.
  • Failed scrape    → NOT acked; Pub/Sub redelivers until ack deadline / DLQ.
  • Malformed msgs   → acked immediately so they don't block the subscription.
  • DB commit fails  → task raises before ack; all messages redelivered.
"""

from __future__ import annotations

import base64
import json
import logging
from datetime import datetime, timedelta

from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────
GCP_PROJECT_ID = "vlr-analytics"
GCP_CONN_ID = "google_cloud_default"
SUBSCRIPTION = "vlr-stats-scraper-completion-sub"
POSTGRES_CONN = "vlr_metadata_postgres"
TABLE = "vlr_events_metadata"
MAX_MESSAGES = 200


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def _decode_payload(raw_msg: dict) -> dict | None:
    """
    Safely decode a single Pub/Sub message payload.
    Returns None on any decoding error so the caller can skip the message
    without aborting the entire batch.
    """
    try:
        data = raw_msg["message"]["data"]
        return json.loads(base64.b64decode(data).decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "Could not decode message ack_id=%s: %s", raw_msg.get("ack_id"), exc
        )
        return None


# ─────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────
@dag(
    dag_id="gud_vlr_stats_scraper_completion",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=20),
        "retry_exponential_backoff": True,
    },
    tags=["vlr", "completion"],
    doc_md="""
## VLR Stats Scraper — Completion

Runs every **5 minutes**. Pulls up to 200 messages from
`vlr-stats-scraper-completion-sub`, marks successful rows as
`is_scrapped = TRUE` in a single Postgres transaction, then acks those
messages.

**Failure handling**
- Failed scrape messages are **not acked** → redelivered by Pub/Sub (configure
  a dead-letter topic on the subscription for permanent failures).
- If the DB commit fails the task raises before ack → full redelivery.
- Malformed messages are acked immediately to avoid blocking the subscription.
    """,
)
def completion() -> None:

    # ──────────────────────────────────────────
    # TASK 1 — Pull (no ack yet)
    # ──────────────────────────────────────────
    pull = PubSubPullSensor(
        task_id="wait_for_completion_messages",
        project_id=GCP_PROJECT_ID,
        subscription=SUBSCRIPTION,
        max_messages=MAX_MESSAGES,
        ack_messages=False,  # manual ack after DB commit
        deferrable=True,  # releases the worker slot while waiting
        poke_interval=30,
    )

    # ──────────────────────────────────────────
    # TASK 2 — Parse & triage
    # ──────────────────────────────────────────
    @task
    def parse_and_triage(raw_msgs: list[dict]) -> dict:
        """
        Splits messages into:
          • to_mark  — successful scrapes; will be committed then acked.
          • to_ack   — ack_ids to send after a successful DB commit
                       (successes + malformed; failures excluded → redelivered).
        """
        to_mark: list[int] = []  # row_ids
        to_ack: list[str] = []  # ack_ids

        for raw in raw_msgs:
            ack_id = raw.get("ack_id")
            payload = _decode_payload(raw)

            if payload is None:
                # Malformed — ack to unblock the subscription, skip DB write.
                if ack_id:
                    to_ack.append(ack_id)
                continue

            if payload.get("success"):
                to_mark.append(payload["row_id"])
                to_ack.append(ack_id)
            else:
                # Failed scrape — do not ack; Pub/Sub will redeliver.
                log.warning(
                    "Scrape failed — row_id=%s error=%s (will be redelivered)",
                    payload.get("row_id"),
                    payload.get("error"),
                )

        log.info(
            "Triaged %d messages: %d to mark scraped, %d to ack, %d redelivered.",
            len(raw_msgs),
            len(to_mark),
            len(to_ack),
            len(raw_msgs) - len(to_ack),
        )

        return {"to_mark": to_mark, "to_ack": to_ack}

    # ──────────────────────────────────────────
    # TASK 3 — Commit DB in one transaction
    # ──────────────────────────────────────────
    @task
    def update_database(triage: dict) -> dict:
        """
        Marks all successful rows in a single transaction.
        If this raises, the ack task will not run and Pub/Sub redelivers.
        """
        to_mark: list[int] = triage["to_mark"]

        if not to_mark:
            log.info("No successful rows to mark — skipping DB write.")
            return triage

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {TABLE}
                    SET    is_scrapped  = TRUE,
                           last_scraped = NOW()
                    WHERE  id = ANY(%(ids)s)
                    """,
                    {"ids": to_mark},
                )
            conn.commit()

        log.info("Marked %d rows as scraped.", len(to_mark))
        return triage  # pass through so ack_messages receives to_ack

    # ──────────────────────────────────────────
    # TASK 4 — Ack only after DB commit succeeds
    # ──────────────────────────────────────────
    @task
    def ack_messages(triage: dict) -> None:
        """
        Acknowledges only messages for successfully processed or malformed
        rows. Failed scrape messages are intentionally excluded.
        """
        ack_ids: list[str] = triage["to_ack"]

        if not ack_ids:
            log.info("No messages to acknowledge.")
            return

        PubSubHook(gcp_conn_id=GCP_CONN_ID).acknowledge(
            project_id=GCP_PROJECT_ID,
            subscription=SUBSCRIPTION,
            ack_ids=ack_ids,
        )
        log.info("Acknowledged %d Pub/Sub messages.", len(ack_ids))

    # ──────────────────────────────────────────
    # WIRING
    # ──────────────────────────────────────────
    triaged = parse_and_triage(pull.output)
    committed = update_database(triaged)
    ack_messages(committed)


completion()
