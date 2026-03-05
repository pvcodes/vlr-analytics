from pyspark.sql import types as T


BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("player_id", T.IntegerType(), nullable=False),
        T.StructField("player", T.StringType(), nullable=False),
        T.StructField("org", T.StringType(), nullable=True),
        T.StructField("agents", T.StringType(), nullable=True),
        T.StructField("rounds_played", T.IntegerType(), nullable=True),
        T.StructField("rating", T.DoubleType(), nullable=True),
        T.StructField("average_combat_score", T.DoubleType(), nullable=True),
        T.StructField("kill_deaths", T.DoubleType(), nullable=True),
        T.StructField("kill_assists_survived_traded", T.StringType(), nullable=True),
        T.StructField("average_damage_per_round", T.DoubleType(), nullable=True),
        T.StructField("kills_per_round", T.StringType(), nullable=True),
        T.StructField("assists_per_round", T.StringType(), nullable=True),
        T.StructField("first_kills_per_round", T.StringType(), nullable=True),
        T.StructField("first_deaths_per_round", T.DoubleType(), nullable=True),
        T.StructField("headshot_percentage", T.StringType(), nullable=True),
        T.StructField("clutch_success_percentage", T.StringType(), nullable=True),
        T.StructField("clutches_won_played_ratio", T.StringType(), nullable=True),
        T.StructField("max_kills_in_single_map", T.IntegerType(), nullable=True),
        T.StructField("kills", T.IntegerType(), nullable=True),
        T.StructField("deaths", T.IntegerType(), nullable=True),
        T.StructField("assists", T.IntegerType(), nullable=True),
        T.StructField("first_kills", T.IntegerType(), nullable=True),
        T.StructField("first_deaths", T.IntegerType(), nullable=True),
        # Partition columns — injected by basePath
        T.StructField("event_id", T.IntegerType(), nullable=False),
        T.StructField("region", T.StringType(), nullable=False),
        T.StructField("map", T.StringType(), nullable=False),
        T.StructField("agent", T.StringType(), nullable=False),
        T.StructField("snapshot_date", T.DateType(), nullable=False),
    ]
)
