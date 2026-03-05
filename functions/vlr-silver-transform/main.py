"""
silver_pipeline.py
------------------
Bronze → Silver pipeline for Dataproc Serverless.

SUBMIT COMMAND:
    gcloud dataproc batches submit pyspark gs://YOUR_BUCKET/jobs/silver_pipeline.py \
        --region=YOUR_REGION \
        --deps-bucket=gs://YOUR_BUCKET/dataproc-deps \
        --jars=gs://YOUR_BUCKET/drivers/postgresql-42.7.3.jar \
        -- \
        --snapshot_date=2026-02-28 \
        --bronze_path=gs://YOUR_BUCKET/data/bronze \
        --silver_path=gs://YOUR_BUCKET/data/silver \
        --jdbc_url=jdbc:postgresql://HOST:PORT/DATABASE \
        --jdbc_user=YOUR_USER \
        --jdbc_password=YOUR_PASSWORD \
        --jdbc_table=agents

NOTES:
    - Upload the PostgreSQL JDBC jar to GCS first:
        gsutil cp postgresql-42.7.3.jar gs://YOUR_BUCKET/drivers/
    - Upload this script to GCS before submitting:
        gsutil cp silver_pipeline.py gs://YOUR_BUCKET/jobs/
    - --deps-bucket is a GCS bucket Dataproc uses for staging dependencies
"""

import argparse
import logging
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T


from utils.schema import BRONZE_SCHEMA
from utils.helper import cast_percentage, cast_ratio_string, ratio_mismatch

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("silver_pipeline")


# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser(description="VLR Bronze → Silver pipeline")

    parser.add_argument(
        "--snapshot_date",
        required=True,
        help="Snapshot date to process (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--bronze_path",
        required=True,
        help="GCS path to Bronze layer. e.g. gs://bucket/data/bronze",
    )
    parser.add_argument(
        "--silver_path",
        required=True,
        help="GCS path to write Silver layer. e.g. gs://bucket/data/silver",
    )
    parser.add_argument(
        "--jdbc_url",
        required=True,
        help="JDBC URL for PostgreSQL. e.g. jdbc:postgresql://host:5432/db",
    )
    parser.add_argument("--jdbc_user", required=True)
    parser.add_argument("--jdbc_password", required=True)
    parser.add_argument(
        "--jdbc_table",
        default="agents",
        help="Table name with agent/role columns (default: agents)",
    )
    parser.add_argument(
        "--ratio_tolerance",
        type=float,
        default=0.05,
        help="Deviation tolerance for dq_ratio_mismatch flag (default: 0.05)",
    )
    parser.add_argument(
        "--min_rounds",
        type=int,
        default=50,
        help="Minimum rounds_played threshold for dq_low_sample flag (default: 50)",
    )

    return parser.parse_args()


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------


def create_spark_session() -> SparkSession:
    """
    On Dataproc Serverless, getOrCreate() reuses the cluster session.
    Master and executor config are handled by Dataproc — don't set them here.
    """
    spark = (
        SparkSession.builder.appName("vlr_bronze_to_silver")
        # mergeSchema handles partition files written with slightly different schemas
        .config("spark.sql.parquet.mergeSchema", "true").getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Pipeline Steps
# ---------------------------------------------------------------------------


def read_bronze(spark: SparkSession, bronze_path: str, snapshot_date: str):
    snapshot_glob = (
        f"{bronze_path}/"
        f"event_id=*/region=*/map=*/agent=*/"
        f"snapshot_date={snapshot_date}/"
    )
    return (
        spark.read.option("header", "true")
        .option("basePath", bronze_path)
        .schema(BRONZE_SCHEMA)
        .csv(snapshot_glob)
    )


def read_agent_roles(
    spark: SparkSession, jdbc_url: str, user: str, password: str, table: str
):
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
        .withColumn("agent", F.lower(F.trim(F.col("agent"))))
        .select("agent", "role")
    )


def transform(df, agent_roles_df, ratio_tolerance: float, min_rounds: int):
    # ── Step 1: Rename + string normalization ──────────────────
    df = df.withColumnRenamed("kill_deaths", "kill_death_ratio")

    for col in ["agent", "map", "region"]:
        df = df.withColumn(col, F.lower(F.trim(F.col(col))))
    for col in ["player", "org"]:
        df = df.withColumn(col, F.trim(F.col(col)))

    # ── Step 2: Capture null flags BEFORE casting ──────────────
    # Must be set here — casting will transform/fill nulls downstream
    df = df.withColumn(
        "dq_clutch_no_attempts",
        F.col("clutch_success_percentage").isNull()
        & F.col("clutches_won_played_ratio").isNull(),
    )

    # ── Step 3: Type casting ───────────────────────────────────
    df = df.withColumn(
        "kill_assists_survived_traded", cast_percentage("kill_assists_survived_traded")
    )
    df = df.withColumn("headshot_percentage", cast_percentage("headshot_percentage"))
    df = df.withColumn(
        "clutch_success_percentage", cast_percentage("clutch_success_percentage")
    )

    df = df.withColumn("kills_per_round", F.col("kills_per_round").cast(T.DoubleType()))
    df = df.withColumn(
        "assists_per_round", F.col("assists_per_round").cast(T.DoubleType())
    )
    df = df.withColumn(
        "first_kills_per_round", F.col("first_kills_per_round").cast(T.DoubleType())
    )

    df = df.withColumn(
        "clutches_won_played_ratio", cast_ratio_string("clutches_won_played_ratio")
    )

    df = df.withColumn("player_id", F.col("player_id").cast(T.IntegerType()))
    df = df.withColumn("rounds_played", F.col("rounds_played").cast(T.IntegerType()))
    df = df.withColumn("rating", F.col("rating").cast(T.DoubleType()))
    df = df.withColumn(
        "average_combat_score", F.col("average_combat_score").cast(T.DoubleType())
    )
    df = df.withColumn(
        "kill_death_ratio", F.col("kill_death_ratio").cast(T.DoubleType())
    )
    df = df.withColumn(
        "average_damage_per_round",
        F.col("average_damage_per_round").cast(T.DoubleType()),
    )
    df = df.withColumn(
        "first_deaths_per_round", F.col("first_deaths_per_round").cast(T.DoubleType())
    )
    df = df.withColumn(
        "max_kills_in_single_map",
        F.col("max_kills_in_single_map").cast(T.IntegerType()),
    )
    df = df.withColumn("kills", F.col("kills").cast(T.IntegerType()))
    df = df.withColumn("deaths", F.col("deaths").cast(T.IntegerType()))
    df = df.withColumn("assists", F.col("assists").cast(T.IntegerType()))
    df = df.withColumn("first_kills", F.col("first_kills").cast(T.IntegerType()))
    df = df.withColumn("first_deaths", F.col("first_deaths").cast(T.IntegerType()))
    df = df.withColumn("snapshot_date", F.col("snapshot_date").cast(T.DateType()))

    # ── Step 4: Remaining DQ flags + clutch null fill ──────────
    df = df.withColumn(
        "dq_low_sample",
        F.col("rounds_played").isNull() | (F.col("rounds_played") < min_rounds),
    )

    # Fill clutch nulls AFTER the dq flag was already captured in Step 2
    df = df.withColumn(
        "clutch_success_percentage",
        F.coalesce(F.col("clutch_success_percentage"), F.lit(0.0)),
    )
    df = df.withColumn(
        "clutches_won_played_ratio",
        F.coalesce(F.col("clutches_won_played_ratio"), F.lit(0.0)),
    )

    df = df.withColumn(
        "dq_null_core_fields",
        F.col("rating").isNull()
        | F.col("average_combat_score").isNull()
        | F.col("kills").isNull(),
    )

    df = df.withColumn(
        "dq_ratio_mismatch",
        ratio_mismatch("kill_death_ratio", "kills", "deaths", ratio_tolerance)
        | ratio_mismatch("kills_per_round", "kills", "rounds_played", ratio_tolerance)
        | ratio_mismatch(
            "first_kills_per_round", "first_kills", "rounds_played", ratio_tolerance
        )
        | ratio_mismatch(
            "first_deaths_per_round", "first_deaths", "rounds_played", ratio_tolerance
        ),
    )

    # ── Step 5: Derived metrics ────────────────────────────────
    df = df.withColumn(
        "fk_fd_ratio",
        F.when(
            F.col("first_deaths").isNull() | F.col("first_kills").isNull(),
            F.lit(None).cast(T.DoubleType()),
        )
        .when((F.col("first_deaths") == 0) & (F.col("first_kills") > 0), F.lit(99.0))
        .when(
            (F.col("first_deaths") == 0) & (F.col("first_kills") == 0),
            F.lit(None).cast(T.DoubleType()),
        )
        .otherwise(F.round(F.col("first_kills") / F.col("first_deaths"), 3)),
    )

    df = df.withColumn(
        "net_first_blood",
        F.when(
            F.col("first_kills").isNotNull() & F.col("first_deaths").isNotNull(),
            F.col("first_kills") - F.col("first_deaths"),
        ).otherwise(F.lit(None).cast(T.IntegerType())),
    )

    df = df.withColumn(
        "damage_delta",
        F.when(
            F.col("average_damage_per_round").isNotNull(),
            F.round(F.col("average_damage_per_round") - 150.0, 2),
        ).otherwise(F.lit(None).cast(T.DoubleType())),
    )

    # ── Step 6: Agent role enrichment ─────────────────────────
    df = df.join(F.broadcast(agent_roles_df), on="agent", how="left")

    # ── Step 7: Final column order ─────────────────────────────
    df = df.select(
        [
            "player_id",
            "player",
            "org",
            "event_id",
            "region",
            "map",
            "agent",
            "role",
            "snapshot_date",
            "rounds_played",
            "rating",
            "average_combat_score",
            "kill_death_ratio",
            "kill_assists_survived_traded",
            "average_damage_per_round",
            "kills_per_round",
            "assists_per_round",
            "first_kills_per_round",
            "first_deaths_per_round",
            "headshot_percentage",
            "clutch_success_percentage",
            "clutches_won_played_ratio",
            "max_kills_in_single_map",
            "fk_fd_ratio",
            "net_first_blood",
            "damage_delta",
            "kills",
            "deaths",
            "assists",
            "first_kills",
            "first_deaths",
            "dq_low_sample",
            "dq_clutch_no_attempts",
            "dq_null_core_fields",
            "dq_ratio_mismatch",
        ]
    )

    return df


def log_dq_summary(df, ratio_tolerance: float, min_rounds: int):
    """Log DQ flag counts — runs a count per flag for observability."""
    total = df.count()
    logger.info(f"Total Silver rows: {total:,}")
    logger.info(f"  Ratio tolerance : {ratio_tolerance}")
    logger.info(f"  Min rounds      : {min_rounds}")
    logger.info("-" * 45)
    for flag in [
        "dq_low_sample",
        "dq_clutch_no_attempts",
        "dq_null_core_fields",
        "dq_ratio_mismatch",
    ]:
        count = df.filter(F.col(flag) == True).count()
        pct = (count / total * 100) if total > 0 else 0
        logger.info(f"  {flag:<28} {count:>6,}  ({pct:.1f}%)")


def write_silver(df, silver_path: str):
    (
        df.write.format("parquet")
        .mode("overwrite")
        .partitionBy("event_id", "region")
        .save(silver_path)
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    args = parse_args()
    start = time.time()

    logger.info("=" * 60)
    logger.info("VLR Silver Pipeline — Dataproc Serverless")
    logger.info("=" * 60)
    logger.info(f"  snapshot_date : {args.snapshot_date}")
    logger.info(f"  bronze_path   : {args.bronze_path}")
    logger.info(f"  silver_path   : {args.silver_path}")

    spark = create_spark_session()

    # Read
    logger.info("[1/4] Reading Bronze...")
    df = read_bronze(spark, args.bronze_path, args.snapshot_date)
    logger.info(f"  Bronze rows loaded: {df.count():,}")

    logger.info("[2/4] Reading agent_roles from PostgreSQL...")
    agent_roles_df = read_agent_roles(
        spark, args.jdbc_url, args.jdbc_user, args.jdbc_password, args.jdbc_table
    )
    logger.info(f"  Agent roles loaded: {agent_roles_df.count()} agents")

    # Transform
    logger.info("[3/4] Running transformations...")
    df = transform(df, agent_roles_df, args.ratio_tolerance, args.min_rounds)

    # Log DQ summary before writing
    log_dq_summary(df, args.ratio_tolerance, args.min_rounds)

    # Write
    logger.info("[4/4] Writing Silver...")
    write_silver(df, args.silver_path)

    elapsed = time.time() - start
    logger.info("=" * 60)
    logger.info(f"Pipeline complete — {elapsed:.1f}s")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
