#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlr Analytics — Bronze to Silver Transformation
------------------------------------------------
Reads a single snapshot partition from GCS Bronze layer, applies cleaning
and type transformations, and writes Parquet to GCS Silver layer.

Usage:
    python main.py \
        --base_path gs://vlr-data-lake/bronze \
        --silver_path gs://vlr-data-lake/silver \
        --snapshot_date 2026-02-26

Airflow (DataprocCreateBatchOperator args):
    ["--base_path", "gs://...", "--silver_path", "gs://...", "--snapshot_date", "{{ ds }}"]
"""

import argparse
import logging
import sys

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
    sum as spark_sum,
)
from pyspark.sql.types import IntegerType, DoubleType


def get_logger(name: str) -> logging.Logger:
    """
    GCP/Dataproc-friendly logger.
    - Logs to stdout so Dataproc Serverless captures them in Cloud Logging.
    - Suppresses noisy py4j and pyspark loggers.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(logging.INFO)
    logger.propagate = False

    for noisy in ("py4j", "pyspark", "org.apache.spark"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return logger


logger = get_logger(__name__)

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
    logger.info(
        "Reading bronze | snapshot_date=%s path=%s", snapshot_date, snapshot_glob
    )

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("basePath", base_path)
        .csv(snapshot_glob)
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingested_at", current_timestamp())
        .drop("agents")
    )
    count = df.count()
    logger.info("Bronze read complete | row_count=%d", count)
    return df


def deduplicate(df):
    """
    Dedup on composite natural key using dropDuplicates.
    Avoids Window + row_number shuffle which caused MetadataFetchFailedException
    under low executor counts. dropDuplicates uses a hash aggregate — lighter
    on memory and shuffle output locations.
    """
    logger.info("Deduplicating | key=%s", COMPOSITE_KEY)
    before = df.count()
    df = df.dropDuplicates(COMPOSITE_KEY)
    after = df.count()
    logger.info(
        "Dedup complete | before=%d after=%d dropped=%d",
        before,
        after,
        before - after,
    )
    return df


def derive_nulls(df):
    """
    Derive columns that can be computed from existing raw counts
    rather than leaving as NULL.
    """
    logger.info("Deriving null columns: kill_deaths, first_kills_per_round")
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
    NULLs are preserved — unknown != zero.
    """
    logger.info("Normalizing percentage columns | cols=%s", PCT_COLS)
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

    clutch_success_percentage dropped — VLR.gg leaves it NULL when
    clutches_won=0, making it unreliable. Derived rate returns 0.0 correctly.
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
    logger.info("Trimming string columns: player, org")
    df = df.withColumn("player", trim(col("player"))).withColumn(
        "org", trim(col("org"))
    )
    return df


def add_metadata(df, pipeline_version):
    logger.info("Adding lineage metadata | pipeline_version=%s", pipeline_version)
    df = df.withColumn("_pipeline_version", lit(pipeline_version))
    return df


def validate(df):
    """
    Log null counts per column for observability.
    No rows are dropped — validation is informational at Silver layer.
    """
    logger.info("Running null validation")
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
    dirty = {c: n for c, n in null_counts.items() if n > 0}
    if dirty:
        for column, count in dirty.items():
            logger.warning("Null detected | column=%s null_count=%d", column, count)
    else:
        logger.info("Null validation passed | no nulls detected")
    return df


def write_silver(df, silver_path):
    logger.info("Writing silver | path=%s", silver_path)
    (
        df.coalesce(4)
        .write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy(
            "snapshot_date", "event_id", "region", "map", "agent"
        )  # snapshot_date first
        .parquet(silver_path)
    )
    logger.info("Silver write complete")


def main():
    args = parse_args()

    logger.info(
        "Pipeline starting | snapshot_date=%s pipeline_version=%s",
        args.snapshot_date,
        args.pipeline_version,
    )

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

    logger.info("Pipeline complete | snapshot_date=%s", args.snapshot_date)
    spark.stop()


if __name__ == "__main__":
    main()
c
