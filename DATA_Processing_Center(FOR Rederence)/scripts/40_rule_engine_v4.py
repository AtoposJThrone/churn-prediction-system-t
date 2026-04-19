"""
40_rule_engine_v4.py
====================
Rule-model hybrid churn early warning entry point.

Pipeline:
  1. Rule engine (hard rules) -> force high risk if matched
  2. Spark RF model scoring -> risk level by probability threshold
  3. Churn reason analysis (rule descriptions + feature heuristics)
  4. Write results to CSV / Hive / MySQL

Logic delegated to:
  - alert.rules    : rule definitions, apply_rules(), get_top_reasons()
  - alert.scoring  : Pipeline + RF model load, user scoring
  - alert.writers  : CSV / Hive / MySQL persistence

Run:
  spark-submit --master yarn --deploy-mode client 40_rule_engine_v4.py
"""

import pandas as pd

from common.spark_helper import build_spark_session
from alert.scoring import score_users
from alert.writers import (
    write_alert_result, write_daily_summary, write_map_hotspot,
)


def run_alert_pipeline():
    RUN_DT = pd.Timestamp.today().strftime("%Y-%m-%d")

    spark = build_spark_session("TowerDefense_RuleEngine",
                                enable_hive=True, shuffle_partitions=100)
    try:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    except Exception:
        pass

    result_df, feature_df, result_sdf = score_users(spark, RUN_DT)

    write_alert_result(spark, result_df, result_sdf, RUN_DT)
    write_daily_summary(spark, result_df, feature_df, RUN_DT)
    write_map_hotspot(spark, result_sdf, RUN_DT)

    spark.stop()
    print("\nAlert pipeline complete.")


if __name__ == "__main__":
    run_alert_pipeline()
