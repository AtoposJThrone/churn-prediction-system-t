#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
14_ods_loader.py
================
ODS layer data ingestion entry point.

Execution order (on master node):
  1. python3 11_data_transform.py
  2. spark-submit --master yarn scripts/14_ods_loader.py
  3. spark-submit scripts/20_etl_pipeline_v3.py

Logic is delegated to submodules:
  - ods.schema   : Hive DDL definitions
  - ods.loaders  : HDFS upload, CSV read, ODS write, verification
  - common.spark_helper : Spark session builder, HDFS/MySQL utilities
"""

from datetime import datetime

from common.spark_helper import build_spark_session
from ods.schema import ensure_hive_tables
from ods.loaders import (
    upload_to_hdfs,
    load_battle_log,
    load_map_meta,
    load_user_label,
    verify_ods,
)

RUN_DATE = datetime.today().strftime("%Y-%m-%d")


def main():
    spark = build_spark_session("TD_ODS_Loader", enable_hive=True,
                                shuffle_partitions=50)

    ensure_hive_tables(spark)
    upload_to_hdfs()
    load_battle_log(spark)
    load_map_meta(spark)
    load_user_label(spark)
    verify_ods(spark)

    spark.stop()
    print(f"\nODS ingestion complete.  RUN_DATE={RUN_DATE}")


if __name__ == "__main__":
    main()