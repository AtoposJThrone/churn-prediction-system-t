"""
etl.ads
=======
ADS (Application Data Service) layer: join DWS features with user labels,
write Parquet splits for training, and push summary to MySQL.

Bug-fix log:
  Bug-4: removed incorrect insertInto to ads_user_churn_risk
         (ADS risk table is populated by rule_engine, not ETL)
  Bug-5: quality_check uses single-pass aggregation instead of per-column count
"""

from pyspark.sql import functions as F

from config_env import env, env_bool
from common.spark_helper import run_cmd

HIVE_DB    = env("TD_CHURN_HIVE_DB", "td_churn")
USE_HIVE   = env_bool("TD_CHURN_USE_HIVE", True)
OUTPUT_DIR = env("TD_CHURN_ETL_OUTPUT_DIR",
                 env("TD_CHURN_PROJECT_DIR", "/home/hadoop/td_churn_project")
                 + "/etl_output_v2")

# NO ； ONLY & !!!!!!!!!
MYSQL_URL      = env("TD_CHURN_DB_URL", required=True).replace("&amp;", "&").replace(";", "&")
MYSQL_USER     = env("TD_CHURN_DB_USER", required=True)
MYSQL_PASSWORD = env("TD_CHURN_DB_PASSWORD", required=True)
MYSQL_DRIVER   = "com.mysql.cj.jdbc.Driver"


def build_ads_features(spark, dws_all, feat_d1, feat_d3, feat_d7, user_labels):
    """
    Join all DWS tables with user labels to build the final ADS feature table.

    Returns a DataFrame with user_id, label, split, and all feature columns.
    """
    print("\n[ADS] Joining labels ...")

    # Re-read from Hive to avoid OOM on cached DataFrames
    if USE_HIVE:
        dws_all = spark.table(f"{HIVE_DB}.dws_user_battle_stats")
        feat_d1 = spark.table(f"{HIVE_DB}.dws_user_d1_stats")
        feat_d3 = spark.table(f"{HIVE_DB}.dws_user_d3_stats")
        feat_d7 = spark.table(f"{HIVE_DB}.dws_user_d7_stats")

    feat_all = dws_all \
        .join(feat_d1, on="user_id", how="left") \
        .join(feat_d3, on="user_id", how="left") \
        .join(feat_d7, on="user_id", how="left")

    result = user_labels.join(feat_all, on="user_id", how="left")
    fill_cols = [c for c in feat_all.columns
                 if c not in ("user_id", "first_stuck_map_id")]
    result = result.fillna(0.0, subset=fill_cols)

    print(f"  Final columns (incl user_id/label/split): {len(result.columns)}")
    return result


def load_features(spark, result):
    """
    Write feature Parquet splits (train/dev/test) and push a
    summary table to MySQL for the frontend dashboard.
    """
    print("\n[LOAD] Writing Parquet ...")
    for split in ["train", "dev", "test"]:
        out_path = f"{OUTPUT_DIR}/features_{split}.parquet"
        df_split = result.filter(F.col("split") == split).drop("split")
        df_split.coalesce(4).write.mode("overwrite").parquet(out_path)
        print(f"  {split}: {df_split.count():,} users -> {out_path}")

    # Sync HDFS parquet to local so that pandas-based scripts (e.g. 31_eda) can read them
    print("[LOAD] Syncing Parquet from HDFS to local ...")
    run_cmd(["mkdir", "-p", OUTPUT_DIR], check=False)
    for split in ["train", "dev", "test"]:
        hdfs_path = f"{OUTPUT_DIR}/features_{split}.parquet"
        local_path = f"{OUTPUT_DIR}/features_{split}.parquet"
        run_cmd(["hdfs", "dfs", "-get", "-f", hdfs_path, local_path], check=False)
    print("  Local sync done.")

    # Bug-4: do NOT write ads_user_churn_risk here (rule_engine's job)
    # Only write a baseline summary to MySQL for the frontend
    try:
        result.filter(F.col("split").isin("train", "dev")) \
              .select("user_id", "label", "split",
                      "total_battles", "overall_win_rate",
                      "max_map_reached", "active_days",
                      "max_consecutive_fail", "d1_battles",
                      "d1_completed_map1") \
              .write \
              .format("jdbc") \
              .option("url",      MYSQL_URL) \
              .option("dbtable",  "user_feature_summary") \
              .option("user",     MYSQL_USER) \
              .option("password", MYSQL_PASSWORD) \
              .option("driver",   MYSQL_DRIVER) \
              .mode("overwrite").save()
        print("  MySQL -> user_feature_summary written")
    except Exception as e:
        print(f"  [WARN] MySQL skipped: {e}")


def quality_check(result):
    """
    Bug-5 fix: single-pass null-rate aggregation for train split.
    """
    print("\n[QC] Data quality check ...")
    train_df = result.filter(F.col("split") == "train")
    n = train_df.count()

    check_cols = [c for c in train_df.columns
                  if c not in ("user_id", "label", "split")]
    null_row = train_df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in check_cols
    ]).collect()[0].asDict()

    null_rates = sorted(
        [(col, cnt / n) for col, cnt in null_row.items()],
        key=lambda x: -x[1]
    )
    print(f"  Train set: {n:,} users")
    print("  NULL rate Top10:")
    for col_name, rate in null_rates[:10]:
        flag = "WARN " if rate > 0.1 else "OK   "
        print(f"    {flag}{col_name:<45} {rate:.2%}")

    print("\n  Label distribution:")
    result.filter(F.col("split") == "train").groupBy("label").count().show()

    print("  Key feature preview (first 5 rows):")
    result.filter(F.col("split") == "train") \
          .select("user_id", "label", "total_battles", "overall_win_rate",
                  "max_map_reached", "stuck_level_count",
                  "d1_battles", "d1_completed_map1", "max_consecutive_fail") \
          .show(5, truncate=False)
