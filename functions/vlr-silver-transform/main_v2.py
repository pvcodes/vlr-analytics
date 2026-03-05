# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
from pathlib import Path

# %% [markdown]
# ## ENV PATHS

# %%
BASE_DIR_PATH = Path.cwd().parent
JDBC_DRIVER_PATH = f"{BASE_DIR_PATH}/drivers/postgresql-42.7.3.jar"
PG_HOST = "ep-misty-shadow-aipayev7-pooler.c-4.us-east-1.aws.neon.tech"
PG_PORT = 5432
PG_USER = "neondb_owner"
PG_PASSWORD = "npg_O9GyIN8rXwQz"
PG_DATABASE = "vlr_events_metadata"
PG_TABLE = "agents"
jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
print(jdbc_url)

# %%
spark = (
    SparkSession.builder.master("local[*]")
    .config("spark.jars", JDBC_DRIVER_PATH)
    .appName("bronze-to-silver")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# %% [markdown]
# ## Agent Roles from External DB
#
# Read the `agents` table from PostgreSQL.
# We broadcast this later because it's a tiny lookup (~30 rows).

# %%
agent_roles_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", PG_TABLE)
    .option("user", PG_USER)
    .option("password", PG_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
    # Normalize casing in lookup — ensures join works regardless of casing in DB
    .withColumn("agent", F.lower(F.trim(F.col("agent"))))
    .select("agent", "role")
)

print("agent_roles table from PostgreSQL:")
agent_roles_df.show(truncate=False)

# %% [markdown]
# ## Bronze Schema
#
# Schema must match the RAW CSV exactly — string columns stay as StringType
# even if they'll become numeric later. We cast them explicitly after reading.
#
# Partition columns (event_id, region, map, agent, snapshot_date) are included
# because basePath injects them as real columns.
#
# Note: `agents` column is excluded — it was a duplicate of the `agent`
# partition column and has been dropped from Bronze.

# %%
BRONZE_SCHEMA = T.StructType(
    [
        # --- Core player fields ---
        T.StructField("player_id", T.IntegerType(), nullable=False),
        T.StructField("player", T.StringType(), nullable=False),
        T.StructField("org", T.StringType(), nullable=True),
        T.StructField("agents", T.StringType(), nullable=True),
        T.StructField("rounds_played", T.IntegerType(), nullable=True),
        T.StructField("rating", T.DoubleType(), nullable=True),
        T.StructField("average_combat_score", T.DoubleType(), nullable=True),
        # Renamed from kill_deaths → kill_death_ratio after read
        T.StructField("kill_deaths", T.DoubleType(), nullable=True),
        # --- String fields that need parsing (keep as StringType in schema) ---
        T.StructField(
            "kill_assists_survived_traded", T.StringType(), nullable=True
        ),  # "93%"
        T.StructField("average_damage_per_round", T.DoubleType(), nullable=True),
        T.StructField("kills_per_round", T.StringType(), nullable=True),  # "1.13"
        T.StructField("assists_per_round", T.StringType(), nullable=True),  # "0.53"
        T.StructField("first_kills_per_round", T.StringType(), nullable=True),  # "0.20"
        T.StructField("first_deaths_per_round", T.DoubleType(), nullable=True),
        T.StructField("headshot_percentage", T.StringType(), nullable=True),  # "28%"
        T.StructField(
            "clutch_success_percentage", T.StringType(), nullable=True
        ),  # "50%" or NULL
        T.StructField(
            "clutches_won_played_ratio", T.StringType(), nullable=True
        ),  # "1/2" or NULL
        T.StructField("max_kills_in_single_map", T.IntegerType(), nullable=True),
        T.StructField("kills", T.IntegerType(), nullable=True),
        T.StructField("deaths", T.IntegerType(), nullable=True),
        T.StructField("assists", T.IntegerType(), nullable=True),
        T.StructField("first_kills", T.IntegerType(), nullable=True),
        T.StructField("first_deaths", T.IntegerType(), nullable=True),
        # --- Partition columns (injected by basePath) ---
        T.StructField("event_id", T.IntegerType(), nullable=False),
        T.StructField("region", T.StringType(), nullable=False),
        T.StructField("map", T.StringType(), nullable=False),
        T.StructField("agent", T.StringType(), nullable=False),
        T.StructField("snapshot_date", T.DateType(), nullable=False),
    ]
)

# %% [markdown]
# ## Read Bronze

# %%
BRONZE_DATA_PATH = BASE_DIR_PATH / "data" / "bronze"
SNAPSHOT_DATE = "2026-02-28"

snapshot_glob = (
    f"{BRONZE_DATA_PATH}/"
    f"event_id=*/region=*/map=*/agent=*/"
    f"snapshot_date={SNAPSHOT_DATE}/"
)

df = (
    spark.read.option("header", "true")
    .option("basePath", BRONZE_DATA_PATH)
    .schema(BRONZE_SCHEMA)
    .csv(snapshot_glob)
)

print(f"Rows loaded : {df.count():,}")
print(f"Columns     : {len(df.columns)}")
df.printSchema()

# %%
# Inspect raw Bronze string columns before any transforms
df.select(
    "event_id",
    "player",
    "agent",
    "region",
    "map",
    "kill_assists_survived_traded",
    "kills_per_round",
    "headshot_percentage",
    "clutch_success_percentage",
    "clutches_won_played_ratio",
).show(10, truncate=False)

# %% [markdown]
# ## Step 1 — Rename + String Normalization

# %%
# Rename kill_deaths → kill_death_ratio
df = df.withColumnRenamed("kill_deaths", "kill_death_ratio")

# Lowercase + trim categorical partition columns
for col in ["agent", "map", "region"]:
    df = df.withColumn(col, F.lower(F.trim(F.col(col))))

# Trim display columns only
for col in ["player", "org"]:
    df = df.withColumn(col, F.trim(F.col(col)))

print("After normalization:")
df.select("agent", "map", "region", "player", "org").show(5, truncate=False)

# %% [markdown]
# ## Step 2 — Capture Raw Null Flags BEFORE Casting
#
# **Why here?** Casting transforms nulls — `cast_percentage(NULL)` returns `NULL`,
# but `cast_ratio_string("0/2")` returns `0.0` not null.
# We must snapshot which rows had nulls in the RAW Bronze before any casting happens.
#
# This is why `dq_clutch_no_attempts` was always 0 before —
# the flag was set after casting had already filled/transformed the values.

# %%
# Capture raw null state from Bronze BEFORE any casting
# Both must be null → player was never in a clutch situation
# NULL percentage + "0/2" ratio → attempted clutches but won none (not flagged)
df = df.withColumn(
    "dq_clutch_no_attempts",
    F.col("clutch_success_percentage").isNull()
    & F.col("clutches_won_played_ratio").isNull(),
)

# Verify — should match the 51,355 both-null rows we saw in raw Bronze
print(
    f"dq_clutch_no_attempts = True: {df.filter(F.col('dq_clutch_no_attempts')).count():,}"
)

# %% [markdown]
# ## Step 3 — Type Casting
#
# Three patterns from the raw data:
#
# | Pattern | Example | Logic |
# |---------|---------|-------|
# | Percentage string | `"93%"` | strip `%`, cast Double, ÷ 100 |
# | Plain decimal string | `"1.13"` | direct cast to Double |
# | Ratio string | `"1/2"` | split `/` → numerator ÷ denominator |


# %%
def cast_percentage(col_name):
    """
    "93%" → 0.93
    Strips % symbol, casts to Double, divides by 100.
    NULL input → NULL output.
    """
    return F.regexp_replace(F.col(col_name), "%", "").cast(T.DoubleType()) / F.lit(
        100.0
    )


def cast_ratio_string(col_name):
    """
    "1/2" → 0.5  |  "0/2" → 0.0  |  NULL → NULL
    Splits on "/" and divides numerator by denominator.
    denom = 0 → NULL (guard against division by zero).
    """
    numerator = F.split(F.col(col_name), "/").getItem(0).cast(T.DoubleType())
    denominator = F.split(F.col(col_name), "/").getItem(1).cast(T.DoubleType())

    return (
        F.when(F.col(col_name).isNull(), F.lit(None).cast(T.DoubleType()))
        .when(denominator == 0, F.lit(None).cast(T.DoubleType()))
        .otherwise(F.round(numerator / denominator, 4))
    )


print("Helpers defined.")

# %%
# Percentage strings → 0.0-1.0 Double
df = df.withColumn(
    "kill_assists_survived_traded", cast_percentage("kill_assists_survived_traded")
)
df = df.withColumn("headshot_percentage", cast_percentage("headshot_percentage"))
df = df.withColumn(
    "clutch_success_percentage", cast_percentage("clutch_success_percentage")
)

# Plain decimal strings → Double
df = df.withColumn("kills_per_round", F.col("kills_per_round").cast(T.DoubleType()))
df = df.withColumn("assists_per_round", F.col("assists_per_round").cast(T.DoubleType()))
df = df.withColumn(
    "first_kills_per_round", F.col("first_kills_per_round").cast(T.DoubleType())
)

# Ratio string → Double
df = df.withColumn(
    "clutches_won_played_ratio", cast_ratio_string("clutches_won_played_ratio")
)

# Re-cast already-numeric columns — explicit guarantee regardless of Bronze quirks
df = df.withColumn("player_id", F.col("player_id").cast(T.IntegerType()))
df = df.withColumn("rounds_played", F.col("rounds_played").cast(T.IntegerType()))
df = df.withColumn("rating", F.col("rating").cast(T.DoubleType()))
df = df.withColumn(
    "average_combat_score", F.col("average_combat_score").cast(T.DoubleType())
)
df = df.withColumn("kill_death_ratio", F.col("kill_death_ratio").cast(T.DoubleType()))
df = df.withColumn(
    "average_damage_per_round", F.col("average_damage_per_round").cast(T.DoubleType())
)
df = df.withColumn(
    "first_deaths_per_round", F.col("first_deaths_per_round").cast(T.DoubleType())
)
df = df.withColumn(
    "max_kills_in_single_map", F.col("max_kills_in_single_map").cast(T.IntegerType())
)
df = df.withColumn("kills", F.col("kills").cast(T.IntegerType()))
df = df.withColumn("deaths", F.col("deaths").cast(T.IntegerType()))
df = df.withColumn("assists", F.col("assists").cast(T.IntegerType()))
df = df.withColumn("first_kills", F.col("first_kills").cast(T.IntegerType()))
df = df.withColumn("first_deaths", F.col("first_deaths").cast(T.IntegerType()))
df = df.withColumn("snapshot_date", F.col("snapshot_date").cast(T.DateType()))

print("All casts applied.")
df.printSchema()

# %%
# Validate cast results — spot check values are in expected ranges
df.select(
    "kill_assists_survived_traded",  # expect 0.0 - 1.0
    "headshot_percentage",  # expect 0.0 - 1.0
    "clutch_success_percentage",  # expect 0.0 - 1.0 or NULL
    "clutches_won_played_ratio",  # expect 0.0 - 1.0 or NULL
    "kills_per_round",  # expect 0.3 - 1.5 range typically
).show(10)

# %% [markdown]
# ## Step 4 — Remaining DQ Flags + Clutch Null Fill
#
# `dq_clutch_no_attempts` was already set in Step 2 (before casting).
# All other flags run here after casting so comparisons are on numeric values.

# %%
RATIO_TOLERANCE = 0.05  # 5%


def ratio_mismatch(stored_col, numerator_col, denominator_col):
    """
    True if stored VLR ratio deviates > RATIO_TOLERANCE from raw totals.
    Two nullif guards:
      1. denominator → prevents kills/deaths divide by zero
      2. safe_computed → prevents deviation/computed divide by zero
         when computed itself is 0 (e.g. 0 kills / 10 rounds = 0.0)
    """
    denom = F.nullif(F.col(denominator_col), F.lit(0))
    computed = F.col(numerator_col) / denom
    stored = F.col(stored_col)
    safe_computed = F.nullif(computed, F.lit(0.0))
    deviation = F.abs(stored - safe_computed) / F.abs(safe_computed)
    return (
        stored.isNotNull() & safe_computed.isNotNull() & (deviation > RATIO_TOLERANCE)
    )


# dq_low_sample
df = df.withColumn(
    "dq_low_sample", F.col("rounds_played").isNull() | (F.col("rounds_played") < 50)
)

# Fill clutch nulls with 0.0 AFTER dq_clutch_no_attempts was already captured
df = df.withColumn(
    "clutch_success_percentage",
    F.coalesce(F.col("clutch_success_percentage"), F.lit(0.0)),
)
df = df.withColumn(
    "clutches_won_played_ratio",
    F.coalesce(F.col("clutches_won_played_ratio"), F.lit(0.0)),
)

# dq_null_core_fields
df = df.withColumn(
    "dq_null_core_fields",
    F.col("rating").isNull()
    | F.col("average_combat_score").isNull()
    | F.col("kills").isNull(),
)

# dq_ratio_mismatch
df = df.withColumn(
    "dq_ratio_mismatch",
    ratio_mismatch("kill_death_ratio", "kills", "deaths")
    | ratio_mismatch("kills_per_round", "kills", "rounds_played")
    | ratio_mismatch("first_kills_per_round", "first_kills", "rounds_played")
    | ratio_mismatch("first_deaths_per_round", "first_deaths", "rounds_played"),
)

print("DQ flags added.")

# %%
# DQ Summary
total = df.count()
dq_cols = [
    "dq_low_sample",
    "dq_clutch_no_attempts",
    "dq_null_core_fields",
    "dq_ratio_mismatch",
]

print(f"Total rows: {total:,}")
print("-" * 45)
for flag in dq_cols:
    count = df.filter(F.col(flag) == True).count()
    pct = (count / total * 100) if total > 0 else 0
    print(f"  {flag:<28} {count:>6,}  ({pct:.1f}%)")

# %%
# Inspect ratio mismatch rows — understand WHY they're flagged
# before deciding if the flag is meaningful or if tolerance needs adjusting
df.filter(F.col("dq_ratio_mismatch") == True).select(
    "player",
    "kills",
    "deaths",
    "kill_death_ratio",
    F.round(F.col("kills") / F.nullif(F.col("deaths"), F.lit(0)), 3).alias(
        "computed_kd"
    ),
    "kills",
    "rounds_played",
    "kills_per_round",
    F.round(F.col("kills") / F.nullif(F.col("rounds_played"), F.lit(0)), 3).alias(
        "computed_kpr"
    ),
).show(20, truncate=False)

# %% [markdown]
# ## Step 5 — Derived Metrics
#
# Computed from raw totals — not VLR's pre-computed ratios.
#
# | Column | Formula | Meaning |
# |--------|---------|--------|
# | `fk_fd_ratio` | `first_kills / first_deaths` | Entry fragger efficiency. >1 = winning opening duels |
# | `net_first_blood` | `first_kills - first_deaths` | Net opening duel impact per event context |
# | `damage_delta` | `adr - 150` | ADR above/below one kill's worth of damage per round |

# %%
# fk_fd_ratio
# first_deaths=0, first_kills>0 → perfect entry record → sentinel 99.0
# first_deaths=0, first_kills=0 → never entry fragged → NULL
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

# net_first_blood
df = df.withColumn(
    "net_first_blood",
    F.when(
        F.col("first_kills").isNotNull() & F.col("first_deaths").isNotNull(),
        F.col("first_kills") - F.col("first_deaths"),
    ).otherwise(F.lit(None).cast(T.IntegerType())),
)

# damage_delta
df = df.withColumn(
    "damage_delta",
    F.when(
        F.col("average_damage_per_round").isNotNull(),
        F.round(F.col("average_damage_per_round") - 150.0, 2),
    ).otherwise(F.lit(None).cast(T.DoubleType())),
)

# Spot check
df.select(
    "player",
    "first_kills",
    "first_deaths",
    "fk_fd_ratio",
    "net_first_blood",
    "average_damage_per_round",
    "damage_delta",
).show(10)

# %% [markdown]
# ## Step 6 — Agent Role Enrichment
#
# Broadcast join the PostgreSQL lookup onto the Silver DataFrame.
# Left join so unknown agents keep their rows with `role = null`.

# %%
df = df.join(F.broadcast(agent_roles_df), on="agent", how="left")

# Check for agents not in the lookup
unknown = df.filter(F.col("role").isNull()).select("agent").distinct().collect()

if unknown:
    print(f"WARNING — agents not in lookup: {sorted([r['agent'] for r in unknown])}")
    print("Add them to the agents table in PostgreSQL.")
else:
    print("All agents resolved to a role. ✓")

df.select("agent", "role").distinct().orderBy("role", "agent").show(30, truncate=False)

# %% [markdown]
# ## Step 7 — Final Column Order + Schema Review

# %%
# Identity → Context → Volume → Core metrics → Derived → Raw totals → DQ flags
SILVER_COLUMNS = [
    # Identity
    "player_id",
    "player",
    "org",
    # Context
    "event_id",
    "region",
    "map",
    "agent",
    "role",
    "snapshot_date",
    # Volume
    "rounds_played",
    # Core metrics (cleaned)
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
    # Derived metrics
    "fk_fd_ratio",
    "net_first_blood",
    "damage_delta",
    # Raw totals
    "kills",
    "deaths",
    "assists",
    "first_kills",
    "first_deaths",
    # DQ flags
    "dq_low_sample",
    "dq_clutch_no_attempts",
    "dq_null_core_fields",
    "dq_ratio_mismatch",
]

df = df.select(SILVER_COLUMNS)

print(f"Final Silver rows    : {df.count():,}")
print(f"Final Silver columns : {len(df.columns)}")
df.printSchema()

# %% [markdown]
# ## Step 8 — Write Silver (Parquet)
#
# Partitioned by `event_id` and `region`.
# Fewer partitions than Bronze — `snapshot_date`, `map`, `agent` are now
# regular columns, not folder partitions.

# %%
SILVER_PATH = BASE_DIR_PATH / "data" / "silver"

(
    df.write.format("parquet")
    .mode("overwrite")
    .partitionBy("event_id", "region")
    .save(str(SILVER_PATH))
)

print(f"Silver written to: {SILVER_PATH}")

# %%
# Read back and verify
df_verify = spark.read.parquet(str(SILVER_PATH))

print(f"Rows written   : {df.count():,}")
print(f"Rows read back : {df_verify.count():,}")
print()
print("Partitions written:")
import os

for entry in sorted(os.listdir(SILVER_PATH)):
    if not entry.startswith("."):
        print(f"  {entry}")

# %%
spark.stop()
print("Spark session stopped. Silver pipeline complete.")
