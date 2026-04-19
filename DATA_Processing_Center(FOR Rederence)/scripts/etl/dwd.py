"""
etl.dwd
=======
DWD (Data Warehouse Detail) layer: cleansing, dedup, stuck detection,
session splitting, and day_index derivation.

Bug-fix log (from original monolith):
  Bug-1: stuck_group_reset first-row NULL -> added prev_map.isNull() guard
  Bug-2: fail_in_group counted wins in fail streak -> sum(when(result==0,1))
  Bug-3: DWD partitioned write missing dt column -> added dt + saveAsTable
  Bug-8: total_battles column ambiguity in chain joins -> isolated join + drop
  Bug-9: d1_sessions filtered by session_df.day_index which could miss
         cross-midnight sessions -> use min(day_index) within session
"""

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType

from config_env import env, env_bool

HIVE_DB    = env("TD_CHURN_HIVE_DB", "td_churn")
USE_HIVE   = env_bool("TD_CHURN_USE_HIVE", True)
OUTPUT_DIR = env("TD_CHURN_ETL_OUTPUT_DIR",
                 env("TD_CHURN_PROJECT_DIR", "/home/hadoop/td_churn_project")
                 + "/etl_output_v2")

SESSION_GAP_THRESHOLD = 1800
STUCK_FAIL_THRESHOLD  = 2


def extract(spark, data_dir):
    """
    Read source data from ODS (Hive) or local CSV fallback.

    Returns (battle_log, map_meta, user_labels) DataFrames.
    battle_log is enriched with battle_start_ts and battle_date columns.
    """
    from pyspark.sql.types import IntegerType

    print("\n[EXTRACT] Reading ODS layer data ...")

    if USE_HIVE:
        battle_log  = spark.table(f"{HIVE_DB}.ods_battle_log")
        map_meta    = spark.table(f"{HIVE_DB}.ods_map_meta")
        user_labels = spark.table(f"{HIVE_DB}.ods_user_label")
        if "dt" in battle_log.columns:
            battle_log = battle_log.drop("dt")
    else:
        battle_log = spark.read.csv(f"{data_dir}/battle_log.csv",
                                    header=True, inferSchema=True)
        map_meta   = spark.read.csv(f"{data_dir}/map_meta.csv",
                                    header=True, inferSchema=True)
        train = spark.read.csv(f"{data_dir}/train.csv", header=True, inferSchema=True) \
                     .withColumn("split", F.lit("train"))
        dev   = spark.read.csv(f"{data_dir}/dev.csv",   header=True, inferSchema=True) \
                     .withColumn("split", F.lit("dev"))
        test  = spark.read.csv(f"{data_dir}/test.csv",  header=True, inferSchema=True) \
                     .withColumn("label", F.lit(None).cast(IntegerType())) \
                     .withColumn("split", F.lit("test"))
        user_labels = train.unionByName(dev).unionByName(test)

    battle_log = battle_log \
        .withColumn("battle_start_ts",
                    F.unix_timestamp("battle_start_time", "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("battle_date", F.to_date("battle_start_time"))

    battle_log.cache()
    print(f"  battle_log  : {battle_log.count():,} rows")
    print(f"  user_labels : {user_labels.count():,} users")
    return battle_log, map_meta, user_labels


def build_dwd(spark, battle_log, map_meta):
    """
    Build DWD detail table with stuck detection, session splitting,
    and day_index computation.

    Returns (bl, session_df) where bl is the cleaned detail DataFrame.
    """
    print("\n[DWD] Cleansing & detail layer construction ...")

    bl = battle_log \
        .dropDuplicates(["user_id", "map_id", "battle_start_ts"]) \
        .filter(F.col("battle_duration_s").between(1, 7200)) \
        .filter(F.col("base_hp_ratio").between(0, 1))

    bl = bl.join(
        map_meta.select("map_id", "map_clear_rate", "map_avg_retry_times"),
        on="map_id", how="left"
    ).withColumn(
        "difficulty_num",
        F.when(F.col("difficulty_tier") == "easy",    1)
         .when(F.col("difficulty_tier") == "normal",  2)
         .when(F.col("difficulty_tier") == "hard",    3)
         .when(F.col("difficulty_tier") == "extreme", 4)
         .otherwise(2)
    )

    # Backward compatibility: fill missing columns from older ODS data
    for _col, _default in [("is_narrow_win", 0), ("is_first_attempt", 0)]:
        if _col not in bl.columns:
            bl = bl.withColumn(_col, F.lit(_default))

    # ── Stuck detection (Bug-1 & Bug-2 fixes) ──
    w_time = Window.partitionBy("user_id").orderBy("battle_start_ts")

    bl = bl \
        .withColumn("prev_map", F.lag("map_id", 1).over(w_time)) \
        .withColumn(
            "stuck_group_reset",
            # Bug-1: prev_map.isNull() for the first row must start a new group
            F.when(
                F.col("prev_map").isNull() |
                (F.col("map_id") != F.col("prev_map")) |
                (F.col("battle_result") == 1),
                1
            ).otherwise(0)
        ) \
        .withColumn("stuck_group_id",
                    F.sum("stuck_group_reset").over(w_time)) \
        .withColumn(
            "fail_in_group",
            # Bug-2: only count failures, not wins
            F.sum(
                F.when(F.col("battle_result") == 0, F.lit(1)).otherwise(F.lit(0))
            ).over(
                Window.partitionBy("user_id", "stuck_group_id")
                      .orderBy("battle_start_ts")
                      .rowsBetween(Window.unboundedPreceding, 0)
            )
        ) \
        .withColumn("is_stuck",
                    F.when(F.col("fail_in_group") >= STUCK_FAIL_THRESHOLD, 1)
                     .otherwise(0))

    # ── Session splitting ──
    bl = bl \
        .withColumn(
            "is_new_session",
            F.when(
                F.col("session_gap_s").isNull() |
                (F.col("session_gap_s") > SESSION_GAP_THRESHOLD),
                1
            ).otherwise(0)
        ) \
        .withColumn("session_seq", F.sum("is_new_session").over(w_time)) \
        .withColumn("session_id",
                    F.concat_ws("_",
                                F.col("user_id").cast(StringType()),
                                F.col("session_seq").cast(StringType())))

    # ── day_index (Bug-8: isolated join to avoid column ambiguity) ──
    # FIX: renamed reg_date -> first_battle_date for clarity
    #       (this is the earliest battle date, not actual registration date)
    user_first_day = bl.groupBy("user_id") \
                       .agg(F.min("battle_date").alias("first_battle_date"))
    bl = bl.join(user_first_day, on="user_id", how="left") \
           .withColumn("day_index",
                       F.datediff("battle_date", "first_battle_date") + 1) \
           .withColumn("is_day1",
                       F.when(F.col("day_index") == 1, 1).otherwise(0))

    # Drop auxiliary columns
    bl = bl.drop("prev_map", "stuck_group_reset", "stuck_group_id",
                 "fail_in_group", "is_new_session", "session_seq",
                 "first_battle_date")
    bl.cache()
    print(f"  Cleaned rows: {bl.count():,}")

    # ── Bug-3: write partitioned DWD table ──
    if USE_HIVE:
        dwd_cols = [
            "user_id", "map_id", "battle_result", "battle_duration_s",
            "base_hp_ratio", "used_special_tower", "battle_start_time",
            "battle_start_ts", "battle_date", "session_gap_s", "session_id",
            "cumulative_battles", "consecutive_fail", "is_stuck",
            "day_index", "is_day1", "wave_count", "tower_score",
            "difficulty_num", "map_clear_rate", "map_avg_retry_times",
            "is_narrow_win", "is_first_attempt",
        ]
        spark.sql(f"DROP TABLE IF EXISTS {HIVE_DB}.dwd_battle_detail")
        bl.select([c for c in dwd_cols if c in bl.columns]) \
          .withColumn("dt", F.col("battle_date").cast("string")) \
          .write.mode("overwrite").format("orc") \
          .partitionBy("dt") \
          .saveAsTable(f"{HIVE_DB}.dwd_battle_detail")
    else:
        bl.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/dwd_battle_detail")

    # ── Session detail (Bug-9: use min day_index within session) ──
    session_day = bl.groupBy("user_id", "session_id") \
                    .agg(F.min("day_index").alias("day_index"))

    session_df = bl.groupBy("user_id", "session_id", "battle_date").agg(
        F.min("battle_start_ts").alias("session_start_ts"),
        F.max("battle_start_ts").alias("session_end_ts"),
        F.count("*")            .alias("session_battles"),
        F.sum("battle_result")  .alias("session_win_count"),
    ).withColumn(
        "session_duration_s",
        F.col("session_end_ts") - F.col("session_start_ts")
    ).join(session_day, on=["user_id", "session_id"], how="left")

    if USE_HIVE:
        spark.sql(f"DROP TABLE IF EXISTS {HIVE_DB}.dwd_session_detail")
        session_df.withColumn("dt", F.col("battle_date").cast("string")) \
                  .write.mode("overwrite").format("orc") \
                  .partitionBy("dt") \
                  .saveAsTable(f"{HIVE_DB}.dwd_session_detail")
    else:
        session_df.write.mode("overwrite").parquet(
            f"{OUTPUT_DIR}/dwd_session_detail")

    return bl, session_df
