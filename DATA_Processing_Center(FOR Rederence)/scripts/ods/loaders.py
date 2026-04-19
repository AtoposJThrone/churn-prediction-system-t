"""
ods.loaders
===========
Upload local CSV files to HDFS, read them into Spark DataFrames,
and write to Hive ODS tables.
"""

import os

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, FloatType, StringType,
)

from config_env import env
from common.spark_helper import run_cmd, reset_hdfs_dir
from ods.schema import (
    HIVE_DB,
    ODS_MAP_META_LOCATION,
    ODS_USER_LABEL_LOCATION,
)


LOCAL_DATA_DIR = env("TD_CHURN_TRANSFORMED_DIR", "/DataSet_Transformed")
HDFS_LANDING   = env("TD_CHURN_HDFS_LANDING_DIR", "/data/td_churn/landing")


# ──────────────────────── HDFS upload ─────────────────────────

def upload_to_hdfs():
    """Upload local CSV files to HDFS landing directory (idempotent)."""
    print(f"\n[HDFS] Uploading local files to {HDFS_LANDING}")

    run_cmd(["hdfs", "dfs", "-mkdir", "-p", HDFS_LANDING], check=True)

    files_to_upload = [
        "battle_log.csv",
        "map_meta.csv",
        "train.csv",
        "dev.csv",
        "test.csv",
    ]

    for fname in files_to_upload:
        local_path = os.path.join(LOCAL_DATA_DIR, fname)
        hdfs_path  = f"{HDFS_LANDING}/{fname}"

        if not os.path.exists(local_path):
            raise FileNotFoundError(
                f"Local file not found: {local_path}  "
                f"(run 11_data_transform.py first)"
            )

        run_cmd(["hdfs", "dfs", "-rm", "-f", hdfs_path], check=False)

        result = run_cmd(
            ["hdfs", "dfs", "-put", local_path, hdfs_path],
            check=False, capture_output=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"HDFS upload failed: {result.stderr}")

        print(f"  {fname} -> {hdfs_path}")


# ──────────────────── Battle log loader ───────────────────────

BATTLE_LOG_SCHEMA = StructType([
    StructField("user_id",            IntegerType(), True),
    StructField("map_id",             IntegerType(), True),
    StructField("battle_result",      IntegerType(), True),
    StructField("battle_duration_s",  FloatType(),   True),
    StructField("base_hp_ratio",      FloatType(),   True),
    StructField("used_special_tower", IntegerType(), True),
    StructField("battle_start_time",  StringType(),  True),
    StructField("session_gap_s",      FloatType(),   True),
    StructField("cumulative_battles", IntegerType(), True),
    StructField("consecutive_fail",   IntegerType(), True),
    StructField("difficulty_tier",    StringType(),  True),
    StructField("wave_count",         IntegerType(), True),
    StructField("tower_score",        FloatType(),   True),
    StructField("is_narrow_win",      IntegerType(), True),
    StructField("is_first_attempt",   IntegerType(), True),
])


def load_battle_log(spark):
    """
    Read battle_log.csv from HDFS, derive partition column dt from
    the actual battle date (not the ingest date), and write to
    ods_battle_log with idempotent partition overwrite.
    """
    print("\n[ODS] Loading battle_log -> ods_battle_log ...")

    df = spark.read.csv(
        f"{HDFS_LANDING}/battle_log.csv",
        schema=BATTLE_LOG_SCHEMA,
        header=True,
    )

    total = df.count()
    null_users = df.filter(F.col("user_id").isNull()).count()
    print(f"  Total rows: {total:,}   null user_id: {null_users}")

    if null_users > 0:
        print("  Filtering out rows with null user_id")
        df = df.filter(F.col("user_id").isNotNull())

    # dt is derived from the actual battle date (data-date semantics)
    df_out = (
        df.withColumn("dt", F.to_date("battle_start_time").cast("string"))
          .select(
              "user_id", "map_id", "battle_result", "battle_duration_s",
              "base_hp_ratio", "used_special_tower", "battle_start_time",
              "session_gap_s", "cumulative_battles", "consecutive_fail",
              "difficulty_tier", "wave_count", "tower_score",
              "is_narrow_win", "is_first_attempt", "dt",
          )
    )

    # Drop existing partitions for idempotency, then append
    distinct_dts = [row["dt"] for row in df_out.select("dt").distinct().collect()]
    for dt_val in distinct_dts:
        spark.sql(
            f"ALTER TABLE {HIVE_DB}.ods_battle_log "
            f"DROP IF EXISTS PARTITION (dt='{dt_val}')"
        )

    df_out.write.mode("append").insertInto(f"{HIVE_DB}.ods_battle_log")
    spark.sql(f"MSCK REPAIR TABLE {HIVE_DB}.ods_battle_log")

    print(f"  Partitions written: {distinct_dts}   rows: {df_out.count():,}")


# ──────────────────── Map meta loader ─────────────────────────

MAP_META_SCHEMA = StructType([
    StructField("map_id",                 IntegerType(), True),
    StructField("map_avg_duration_s",     FloatType(),   True),
    StructField("map_clear_rate",         FloatType(),   True),
    StructField("map_avg_win_duration_s", FloatType(),   True),
    StructField("map_avg_retry_times",    FloatType(),   True),
    StructField("difficulty_tier",        StringType(),  True),
    StructField("map_design_waves",       IntegerType(), True),
])


def load_map_meta(spark):
    """Read map_meta.csv from HDFS and write to ods_map_meta (full refresh)."""
    print("\n[ODS] Loading map_meta -> ods_map_meta ...")

    df = spark.read.csv(
        f"{HDFS_LANDING}/map_meta.csv",
        schema=MAP_META_SCHEMA,
        header=True,
    )
    cnt = df.count()
    print(f"  Map count: {cnt:,}")

    # External table cannot be TRUNCATEd; clear underlying HDFS dir instead
    print(f"  Clearing external table dir: {ODS_MAP_META_LOCATION}")
    reset_hdfs_dir(ODS_MAP_META_LOCATION)

    df.select(
        "map_id", "map_avg_duration_s", "map_clear_rate",
        "map_avg_win_duration_s", "map_avg_retry_times",
        "difficulty_tier", "map_design_waves",
    ).write.mode("append").insertInto(f"{HIVE_DB}.ods_map_meta")

    print(f"  Written to ods_map_meta: {cnt:,} rows")


# ──────────────────── User label loader ───────────────────────

def load_user_label(spark):
    """
    Read train/dev/test CSVs from HDFS, union them with a split
    column, and write to ods_user_label (full refresh).
    """
    print("\n[ODS] Loading train/dev/test -> ods_user_label ...")

    train = (
        spark.read.csv(f"{HDFS_LANDING}/train.csv", header=True, inferSchema=True)
        .withColumn("split", F.lit("train"))
    )
    dev = (
        spark.read.csv(f"{HDFS_LANDING}/dev.csv", header=True, inferSchema=True)
        .withColumn("split", F.lit("dev"))
    )
    test = (
        spark.read.csv(f"{HDFS_LANDING}/test.csv", header=True, inferSchema=True)
        .withColumn("label", F.lit(None).cast(IntegerType()))
        .withColumn("split", F.lit("test"))
    )

    all_labels = (
        train.select("user_id", "label", "split")
        .unionByName(dev.select("user_id", "label", "split"))
        .unionByName(test.select("user_id", "label", "split"))
    )

    total_users = all_labels.count()
    print(f"  Total users: {total_users:,}")
    print("  Label distribution:")
    all_labels.groupBy("split", "label").count().orderBy("split", "label").show()

    # External table: clear HDFS dir then append
    print(f"  Clearing external table dir: {ODS_USER_LABEL_LOCATION}")
    reset_hdfs_dir(ODS_USER_LABEL_LOCATION)

    all_labels.select("user_id", "label", "split") \
        .write.mode("append") \
        .insertInto(f"{HIVE_DB}.ods_user_label")

    print(f"  Written to ods_user_label: {total_users:,} rows")


# ──────────────────── Verification ────────────────────────────

def verify_ods(spark):
    """Print ODS table counts, partition list, and sample rows for QA."""
    print("\n[VERIFY] ODS verification ...")

    battle_cnt = spark.table(f"{HIVE_DB}.ods_battle_log").count()
    map_cnt    = spark.table(f"{HIVE_DB}.ods_map_meta").count()
    user_cnt   = spark.table(f"{HIVE_DB}.ods_user_label").count()
    print(f"  {'ods_battle_log':<25} total rows: {battle_cnt:,}")
    print(f"  {'ods_map_meta':<25} rows: {map_cnt:,}")
    print(f"  {'ods_user_label':<25} rows: {user_cnt:,}")

    print("\n  battle_log partitions:")
    spark.sql(f"SHOW PARTITIONS {HIVE_DB}.ods_battle_log").show(200, truncate=False)

    sample = (
        spark.table(f"{HIVE_DB}.ods_battle_log")
        .select("battle_start_time", "dt")
        .limit(3).collect()
    )
    print("\n  battle_start_time samples (expected yyyy-MM-dd HH:mm:ss):")
    for row in sample:
        print(f"    {row['battle_start_time']}")

    print("\n  Difficulty tier distribution:")
    spark.table(f"{HIVE_DB}.ods_map_meta") \
         .groupBy("difficulty_tier").count().orderBy("difficulty_tier").show()

    print("\n  User label distribution:")
    spark.table(f"{HIVE_DB}.ods_user_label") \
         .groupBy("split", "label").count().orderBy("split", "label").show()
