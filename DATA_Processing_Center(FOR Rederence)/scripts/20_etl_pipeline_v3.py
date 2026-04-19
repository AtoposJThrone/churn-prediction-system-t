"""
20_etl_pipeline_v3.py
=====================
ETL pipeline entry point (ODS -> DWD -> DWS -> ADS).

Bug-fix history preserved in submodules:
  etl/dwd.py : Bug-1, Bug-2, Bug-3, Bug-8, Bug-9
  etl/dws.py : Bug-6, Bug-7, Bug-8
  etl/ads.py : Bug-4, Bug-5

Logic is delegated to:
  - etl.dwd : extract, build_dwd
  - etl.dws : build_dws
  - etl.ads : build_ads_features, load_features, quality_check
  - common.spark_helper : Spark session builder
"""

import os
from datetime import datetime

from config_env import env

from common.spark_helper import build_spark_session
from etl.dwd import extract, build_dwd
from etl.dws import build_dws
from etl.ads import build_ads_features, load_features, quality_check

PROJECT_DIR = env("TD_CHURN_PROJECT_DIR", "/home/hadoop/td_churn_project")
DATA_DIR    = env("TD_CHURN_TRANSFORMED_DIR", "/DataSet_Transformed")
OUTPUT_DIR  = env("TD_CHURN_ETL_OUTPUT_DIR", f"{PROJECT_DIR}/etl_output_v2")
RUN_DATE    = datetime.today().strftime("%Y-%m-%d")

os.makedirs(OUTPUT_DIR, exist_ok=True)


def main():
    spark = build_spark_session("TowerDefense_Churn_ETL_v2")

    battle_log, map_meta, user_labels = extract(spark, DATA_DIR)
    bl, session_df                    = build_dwd(spark, battle_log, map_meta)
    dws_all, d1, d3, d7              = build_dws(spark, bl, session_df)
    result = build_ads_features(spark, dws_all, d1, d3, d7, user_labels)
    load_features(spark, result)
    quality_check(result)

    spark.stop()
    print(f"\nETL v2 complete.  RUN_DATE={RUN_DATE}")


if __name__ == "__main__":
    main()
