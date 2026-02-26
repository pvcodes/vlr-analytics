#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlr Analytics — Bronze to Silver Transformation
------------------------------------------------
Reads a single snapshot partition from GCS Bronze layer, applies cleaning
and type transformations, and writes Parquet to GCS Silver layer.

Usage:
    python transform.py \
        --base_path gs://vlr-data-lake/bronze \
        --silver_path gs://vlr-data-lake/silver \
        --snapshot_date 2026-02-26

Airflow (DataprocCreateBatchOperator args):
    ["--base_path", "gs://...", "--silver_path", "gs://...", "--snapshot_date", "{{ ds }}"]
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    when,
    round,
    regexp_replace,
    split,
    lit,
    current_timestamp,
    input_file_name,
    row_number,
    sum as spark_sum,
)
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PIPELINE_VERSION = "1.0.0"
COMPOSITE_KEY = ["player_id", "snapshot_date", "agent", "map", "event_id", "region"]
PCT_COLS = [
    "kill_assists_survived_traded",
    "headshot_percentage",
    "clutch_success_percentage",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Bronze to Silver transformation")
    parser.add_argument(
        "--base_path",
        required=True,
        help="GCS root path to Bronze layer e.g. gs://vlr-data-lake/bronze",
    )
    parser.add_argument(
        "--silver_path",
        required=True,
        help="GCS root path to Silver layer e.g. gs://vlr-data-lake/silver",
    )
    parser.add_argument(
        "--snapshot_date",
        required=True,
        help="Snapshot date to process e.g. 2026-02-26",
    )
    parser.add_argument(
        "--pipeline_version", default=PIPELINE_VERSION, help="Pipeline version tag"
    )
    return parser.parse_args()


def read_bronze(spark, base_path, snapshot_date):
    """
    Read only the specific snapshot_date partition using a glob pattern.
    basePath still points to the Bronze root so Spark correctly promotes
    all Hive partition columns (event_id, region, map, agent, snapshot_date).
    """
    snapshot_glob = (
        f"{base_path}/event_id=*/region=*/map=*/agent=*/snapshot_date={snapshot_date}/"
    )
    logger.info(f"Reading snapshot_date={snapshot_date} from: {snapshot_glob}")

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("basePath", base_path)
        .csv(snapshot_glob)
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingested_at", current_timestamp())
        .drop("agents")  # redundant — agent partition column is source of truth
    )
    logger.info(f"Row count after read: {df.count()}")
    return df


def deduplicate(df):
    """
    Defensive dedup on composite natural key.
    Same player can appear multiple times per snapshot across different
    agent/map/region/event combinations — only exact matches on all 6
    fields are true duplicates. Latest ingested row wins.
    """
    logger.info("Applying deduplication on composite key")
    window = Window.partitionBy(*COMPOSITE_KEY).orderBy(col("_ingested_at").desc())
    df = (
        df.withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    logger.info(f"Row count after dedup: {df.count()}")
    return df


def derive_nulls(df):
    """
    Derive columns that can be computed from existing raw counts
    rather than leaving as NULL.

    kill_deaths           — NULL when deaths=0 (division by zero in source)
    first_kills_per_round — NULL when first_kills=0 (VLR.gg omits zero values)
    """
    logger.info("Deriving kill_deaths and first_kills_per_round from raw counts")

    df = df.withColumn(
        "kill_deaths",
        when(
            col("kill_deaths").isNull(),
            when(col("deaths") == 0, col("kills").cast(DoubleType())).otherwise(
                round(col("kills").cast(DoubleType()) / col("deaths"), 2)
            ),
        ).otherwise(col("kill_deaths")),
    )

    df = df.withColumn(
        "first_kills_per_round",
        when(
            col("first_kills_per_round").isNull(),
            round(col("first_kills").cast(DoubleType()) / col("rounds_played"), 2),
        ).otherwise(col("first_kills_per_round")),
    )

    return df


def normalize_percentages(df):
    """
    Strip % suffix and normalize percentage columns to 0-1 float.
    e.g. "89%" -> 0.89
    NULLs are preserved — unknown != zero.
    """
    logger.info("Normalizing percentage columns to 0-1 float")
    for c in PCT_COLS:
        df = df.withColumn(
            c,
            when(
                col(c).isNotNull() & (col(c) != ""),
                regexp_replace(col(c), "%", "").cast(DoubleType()) / 100,
            ).otherwise(None),
        )
    return df


def parse_clutches(df):
    """
    Parse clutches_won_played_ratio string (e.g. "2/3") into:
        clutches_won        -> integer
        clutches_played     -> integer
        clutch_success_rate -> derived double

    clutch_success_percentage is dropped — VLR.gg leaves it NULL whenever
    clutches_won=0, making it unreliable. The derived rate correctly
    returns 0.0 in those cases.
    """
    logger.info("Parsing clutches_won_played_ratio")
    df = (
        df.withColumn(
            "clutches_won",
            when(
                col("clutches_won_played_ratio").isNotNull(),
                split(col("clutches_won_played_ratio"), "/")[0].cast(IntegerType()),
            ).otherwise(None),
        )
        .withColumn(
            "clutches_played",
            when(
                col("clutches_won_played_ratio").isNotNull(),
                split(col("clutches_won_played_ratio"), "/")[1].cast(IntegerType()),
            ).otherwise(None),
        )
        .withColumn(
            "clutch_success_rate",
            when(
                col("clutches_played").isNotNull() & (col("clutches_played") > 0),
                round(
                    col("clutches_won").cast(DoubleType()) / col("clutches_played"), 2
                ),
            ).otherwise(None),
        )
        .drop("clutches_won_played_ratio", "clutch_success_percentage")
    )
    return df


def normalize_strings(df):
    """
    Defensive trim on player and org.
    Casing intentionally preserved — gamertags and org codes are identity fields.
    """
    logger.info("Trimming string columns")
    df = df.withColumn("player", trim(col("player"))).withColumn(
        "org", trim(col("org"))
    )
    return df


def add_metadata(df, pipeline_version):
    logger.info("Adding lineage metadata columns")
    df = df.withColumn("_pipeline_version", lit(pipeline_version))
    return df


def validate(df):
    """
    Log null counts per column for observability.
    No rows are dropped — validation is informational at Silver layer.
    """
    logger.info("Running null count validation")
    null_counts = (
        df.select(
            [
                spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
                for c in df.columns
            ]
        )
        .collect()[0]
        .asDict()
    )
    for column, count in null_counts.items():
        if count > 0:
            logger.warning(f"NULL count — {column}: {count}")
    return df


def write_silver(df, silver_path):
    """
    Write only the current snapshot partition using dynamic partition overwrite.
    Existing snapshot partitions in Silver are untouched.
    """
    logger.info(f"Writing Silver data to: {silver_path}")
    (
        df.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("event_id", "region", "map", "agent", "snapshot_date")
        .parquet(silver_path)
    )
    logger.info("Write complete")


def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("vlr-bronze-to-silver")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    df = read_bronze(spark, args.base_path, args.snapshot_date)
    df = deduplicate(df)
    df = derive_nulls(df)
    df = normalize_percentages(df)
    df = parse_clutches(df)
    df = normalize_strings(df)
    df = add_metadata(df, args.pipeline_version)
    df = validate(df)

    write_silver(df, args.silver_path)

    logger.info(f"Bronze to Silver complete — snapshot_date={args.snapshot_date}")
    spark.stop()


if __name__ == "__main__":
    main()
