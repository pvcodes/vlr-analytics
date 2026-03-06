"""
gold_pipeline.py
----------------
Silver → Gold pipeline for Dataproc Serverless.

Produces three Gold tables:
  - gold_player_performance : player leaderboard per event/region
  - gold_agent_meta         : agent pick rates and performance per event/region
  - gold_map_stats          : map-level performance stats per event/region

All tables:
  - Exclude dq_null_core_fields = True  (unusable rows)
  - Exclude dq_low_sample = True        (statistically noisy rows)
  - Partitioned by event_id
  - Written as Parquet → load to BigQuery

SUBMIT COMMAND:
    gcloud dataproc batches submit pyspark gs://YOUR_BUCKET/jobs/gold_pipeline.py \
        --region=YOUR_REGION \
        --deps-bucket=gs://YOUR_BUCKET/dataproc-deps \
        -- \
        --silver_path=gs://YOUR_BUCKET/data/silver \
        --gold_path=gs://YOUR_BUCKET/data/gold

BIGQUERY LOAD (run after pipeline):
    bq load --source_format=PARQUET --autodetect \
        YOUR_DATASET.gold_player_performance \
        "gs://YOUR_BUCKET/data/gold/gold_player_performance/*.parquet"

    bq load --source_format=PARQUET --autodetect \
        YOUR_DATASET.gold_agent_meta \
        "gs://YOUR_BUCKET/data/gold/gold_agent_meta/*.parquet"

    bq load --source_format=PARQUET --autodetect \
        YOUR_DATASET.gold_map_stats \
        "gs://YOUR_BUCKET/data/gold/gold_map_stats/*.parquet"
"""

import argparse
import logging
import sys
import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("gold_pipeline")


# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser(description="VLR Silver → Gold pipeline")
    parser.add_argument(
        "--silver_path",
        required=True,
        help="GCS path to Silver layer. e.g. gs://bucket/data/silver",
    )
    parser.add_argument(
        "--gold_path",
        required=True,
        help="GCS path to write Gold layer. e.g. gs://bucket/data/gold",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------


def create_spark_session() -> SparkSession:
    spark = SparkSession.builder.appName("vlr_silver_to_gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Read + Filter Silver
# ---------------------------------------------------------------------------


def read_silver(spark: SparkSession, silver_path: str) -> DataFrame:
    """
    Read Silver and apply Gold-layer filters upfront.
    We exclude:
      - dq_null_core_fields = True : rating/acs/kills is null — row is unusable
      - dq_low_sample = True       : rounds_played < 50 — stats are noisy

    Filtering here means all three Gold tables get the same clean base,
    and we only scan Silver once.
    """
    df = spark.read.parquet(silver_path)

    total = df.count()
    df = df.filter(
        (F.col("dq_null_core_fields") == False) & (F.col("dq_low_sample") == False)
    )
    clean = df.count()

    logger.info(f"Silver rows total   : {total:,}")
    logger.info(
        f"Silver rows after DQ filter : {clean:,}  ({total - clean:,} excluded)"
    )

    return df


# ---------------------------------------------------------------------------
# Gold Table 1: gold_player_performance
# ---------------------------------------------------------------------------


def build_player_performance(df: DataFrame) -> DataFrame:
    """
    One row per (player_id, event_id, region).

    Aggregates all map+agent combinations for a player within an event.
    This is the leaderboard table — answers "who performed best in this event?"

    Note on fk_fd_ratio:
      We use avg() but exclude the 99.0 sentinel value (set when first_deaths=0).
      Including 99.0 would inflate averages — it's a special case marker, not
      a real ratio. Players with ALL entries as 99.0 get null avg (no real data).
    """
    return df.groupBy("player_id", "player", "org", "event_id", "region").agg(
        # Volume
        F.sum("rounds_played").alias("total_rounds"),
        F.count("*").alias("map_agent_entries"),  # how many map+agent combos
        F.countDistinct("map").alias("maps_played"),
        F.countDistinct("agent").alias("agents_played"),
        # Core performance — weighted by presence (simple avg across entries)
        F.round(F.avg("rating"), 3).alias("avg_rating"),
        F.round(F.avg("average_combat_score"), 1).alias("avg_acs"),
        F.round(F.avg("kill_death_ratio"), 3).alias("avg_kd"),
        F.round(F.avg("kill_assists_survived_traded"), 3).alias("avg_kast"),
        F.round(F.avg("average_damage_per_round"), 1).alias("avg_adr"),
        F.round(F.avg("headshot_percentage"), 3).alias("avg_hs_pct"),
        # Derived metrics
        F.round(
            F.avg(F.when(F.col("fk_fd_ratio") != 99.0, F.col("fk_fd_ratio"))), 3
        ).alias(
            "avg_fk_fd_ratio"
        ),  # excludes 99.0 sentinel
        F.round(F.avg("damage_delta"), 1).alias("avg_damage_delta"),
        # Raw totals — sum across all map+agent entries
        F.sum("kills").alias("total_kills"),
        F.sum("deaths").alias("total_deaths"),
        F.sum("assists").alias("total_assists"),
        F.sum("first_kills").alias("total_first_kills"),
        F.sum("first_deaths").alias("total_first_deaths"),
        # Clutch
        F.round(F.avg("clutch_success_percentage"), 3).alias("avg_clutch_pct"),
        # Peak performance
        F.max("average_combat_score").alias("peak_acs"),
        F.max("max_kills_in_single_map").alias("max_kills_single_map"),
        # Snapshot date — latest snapshot this player's data came from
        F.max("snapshot_date").alias("snapshot_date"),
    )


# ---------------------------------------------------------------------------
# Gold Table 2: gold_agent_meta
# ---------------------------------------------------------------------------


def build_agent_meta(df: DataFrame) -> DataFrame:
    """
    One row per (agent, event_id, region).

    Answers: "Which agents are being picked, and how are they performing?"
    Useful for meta analysis — e.g. "Is Jett still dominant in NA?"

    pick_count = distinct players who played this agent in this event/region.
    This is more meaningful than row count because one player can appear
    on the same agent across multiple maps.
    """
    return df.groupBy("agent", "role", "event_id", "region").agg(
        # Pick rate signals
        F.countDistinct("player_id").alias("pick_count"),  # unique players
        F.countDistinct("map").alias("maps_appeared"),  # maps it was picked on
        F.sum("rounds_played").alias("total_rounds"),
        # Performance averages across all players who picked this agent
        F.round(F.avg("rating"), 3).alias("avg_rating"),
        F.round(F.avg("average_combat_score"), 1).alias("avg_acs"),
        F.round(F.avg("kill_death_ratio"), 3).alias("avg_kd"),
        F.round(F.avg("kill_assists_survived_traded"), 3).alias("avg_kast"),
        F.round(F.avg("average_damage_per_round"), 1).alias("avg_adr"),
        F.round(F.avg("headshot_percentage"), 3).alias("avg_hs_pct"),
        # Derived
        F.round(
            F.avg(F.when(F.col("fk_fd_ratio") != 99.0, F.col("fk_fd_ratio"))), 3
        ).alias("avg_fk_fd_ratio"),
        F.round(F.avg("damage_delta"), 1).alias("avg_damage_delta"),
        F.max("snapshot_date").alias("snapshot_date"),
    )


# ---------------------------------------------------------------------------
# Gold Table 3: gold_map_stats
# ---------------------------------------------------------------------------


def build_map_stats(df: DataFrame) -> DataFrame:
    """
    One row per (map, event_id, region).

    Answers: "How does this map play? What agents get picked here?"

    most_picked_agent and most_picked_role use a window function:
      - Count entries per (map, event_id, region, agent/role)
      - Rank by count desc within each (map, event_id, region) group
      - Keep rank = 1 → the most picked agent/role on that map

    This is the same window pattern used in Silver deduplication —
    a core PySpark pattern worth understanding well.
    """
    # Base aggregation
    base = df.groupBy("map", "event_id", "region").agg(
        F.sum("rounds_played").alias("total_rounds"),
        F.countDistinct("player_id").alias("unique_players"),
        F.countDistinct("agent").alias("unique_agents"),
        F.round(F.avg("average_combat_score"), 1).alias("avg_acs"),
        F.round(F.avg("average_damage_per_round"), 1).alias("avg_adr"),
        F.round(F.avg("headshot_percentage"), 3).alias("avg_hs_pct"),
        F.round(F.avg("kill_death_ratio"), 3).alias("avg_kd"),
        F.round(F.avg("damage_delta"), 1).alias("avg_damage_delta"),
        F.max("snapshot_date").alias("snapshot_date"),
    )

    # Most picked agent per (map, event_id, region)
    agent_window = Window.partitionBy("map", "event_id", "region").orderBy(
        F.col("agent_count").desc()
    )
    most_picked_agent = (
        df.groupBy("map", "event_id", "region", "agent")
        .agg(F.count("*").alias("agent_count"))
        .withColumn("rank", F.row_number().over(agent_window))
        .filter(F.col("rank") == 1)
        .select("map", "event_id", "region", F.col("agent").alias("most_picked_agent"))
    )

    # Most picked role per (map, event_id, region)
    role_window = Window.partitionBy("map", "event_id", "region").orderBy(
        F.col("role_count").desc()
    )
    most_picked_role = (
        df.groupBy("map", "event_id", "region", "role")
        .agg(F.count("*").alias("role_count"))
        .withColumn("rank", F.row_number().over(role_window))
        .filter(F.col("rank") == 1)
        .select("map", "event_id", "region", F.col("role").alias("most_picked_role"))
    )

    # Join everything together
    return base.join(
        most_picked_agent, on=["map", "event_id", "region"], how="left"
    ).join(most_picked_role, on=["map", "event_id", "region"], how="left")


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------


def write_gold(df: DataFrame, gold_path: str, table_name: str):
    """
    Write a Gold table as Parquet partitioned by event_id.

    WHY partition by event_id only:
      Gold is aggregated — far fewer rows than Silver.
      One partition column is enough. BigQuery will use it for
      partition pruning when analysts filter by event.
    """
    output_path = f"{gold_path}/{table_name}"
    row_count = df.count()

    (
        df.write.format("parquet")
        .mode("overwrite")
        .partitionBy("event_id")
        .save(output_path)
    )

    logger.info(f"  {table_name:<30} {row_count:>8,} rows → {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    args = parse_args()
    start = time.time()

    logger.info("=" * 60)
    logger.info("VLR Gold Pipeline — Dataproc Serverless")
    logger.info("=" * 60)
    logger.info(f"  silver_path : {args.silver_path}")
    logger.info(f"  gold_path   : {args.gold_path}")

    spark = create_spark_session()

    # Read Silver once — all three Gold tables share the same filtered base
    logger.info("[1/5] Reading and filtering Silver...")
    df = read_silver(spark, args.silver_path)

    # Cache — we're about to scan df three times (once per Gold table).
    # Without cache, Spark would re-read and re-filter Silver from GCS three times.
    # With cache, it reads once into memory and reuses it.
    logger.info("[2/5] Caching filtered Silver for reuse...")
    df.cache()
    df.count()  # materialise the cache now so timings below are accurate

    # Build Gold tables
    logger.info("[3/5] Building Gold tables...")
    player_perf = build_player_performance(df)
    agent_meta = build_agent_meta(df)
    map_stats = build_map_stats(df)

    # Write
    logger.info("[4/5] Writing Gold tables...")
    write_gold(player_perf, args.gold_path, "gold_player_performance")
    write_gold(agent_meta, args.gold_path, "gold_agent_meta")
    write_gold(map_stats, args.gold_path, "gold_map_stats")

    # Release cache
    df.unpersist()

    elapsed = time.time() - start
    logger.info("=" * 60)
    logger.info(f"Gold Pipeline complete — {elapsed:.1f}s")
    logger.info("=" * 60)
    logger.info("")
    logger.info("Next step — load to BigQuery:")
    logger.info(f"  bq load --source_format=PARQUET --autodetect \\")
    logger.info(f"    YOUR_DATASET.gold_player_performance \\")
    logger.info(f'    "{args.gold_path}/gold_player_performance/*.parquet"')


if __name__ == "__main__":
    main()
