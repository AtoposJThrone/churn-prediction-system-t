"""
ods_loader.py
=============
ODS 层数据入仓脚本（etl_pipeline_v2.py 的前置步骤）

原 ETL 脚本直接从 Hive ODS 层读表，但没有任何代码把 CSV 文件
写进 ODS——这是整条链路中缺失的第一步。

执行顺序（在 master 上）：
  1. python3 transform_to_td.py        # 生成 data_td/ 目录
  2. python3 ods_loader.py             # CSV → HDFS → Hive ODS
  3. spark-submit etl_pipeline_v2.py   # ODS → DWD → DWS → ADS

运行方式：
  spark-submit \\
    --master yarn \\
    --deploy-mode client \\
    --driver-memory 2g \\
    --executor-memory 1g \\
    --num-executors 2 \\
    ods_loader.py
"""

import os
import subprocess
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, FloatType, StringType, TimestampType, LongType
)

from config_env import env, env_int

# ─────────────────────── 配置 ──────────────────────────────────
LOCAL_DATA_DIR = env("TD_CHURN_TRANSFORMED_DIR", "/DataSet_Transformed")
HDFS_LANDING   = env("TD_CHURN_HDFS_LANDING_DIR", "/data/td_churn/landing")
HIVE_DB        = env("TD_CHURN_HIVE_DB", "td_churn")
WAREHOUSE_DIR  = env("TD_CHURN_HIVE_WAREHOUSE_DIR", "/user/hive/warehouse")
SHUFFLE_PARTITIONS = str(env_int("TD_CHURN_SPARK_SHUFFLE_PARTITIONS", 50))
RUN_DATE       = datetime.today().strftime("%Y-%m-%d")  # 本次入仓分区日期

# ─────────────────────── Spark 初始化 ──────────────────────────
spark = SparkSession.builder \
    .appName("TD_ODS_Loader") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", WAREHOUSE_DIR) \
    .config("hive.exec.dynamic.partition",       "true") \
    .config("hive.exec.dynamic.partition.mode",  "nonstrict") \
    .config("spark.sql.shuffle.partitions",      SHUFFLE_PARTITIONS) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.sql(f"USE {HIVE_DB}")


# ══════════════════════════════════════════════════════════════
# Step 1: 将本地 CSV 上传到 HDFS（幂等，存在则覆盖）
# ══════════════════════════════════════════════════════════════
def upload_to_hdfs():
    print(f"\n[HDFS] 上传本地文件到 HDFS: {HDFS_LANDING}")

    # 确保 HDFS 目录存在
    subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", HDFS_LANDING],
        check=True
    )

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
            raise FileNotFoundError(f"本地文件不存在: {local_path}，请先运行 transform_to_td.py")

        # 删除 HDFS 上的旧文件（如果存在），再上传
        subprocess.run(["hdfs", "dfs", "-rm", "-f", hdfs_path])
        result = subprocess.run(
            ["hdfs", "dfs", "-put", local_path, hdfs_path],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"HDFS 上传失败: {result.stderr}")
        print(f"  ✅ {fname} → {hdfs_path}")


# ══════════════════════════════════════════════════════════════
# Step 2: 读取 CSV，统一类型，写入 ODS
# ══════════════════════════════════════════════════════════════

# ── ODS: battle_log ──
def load_ods_battle_log():
    print("\n[ODS] 加载 battle_log → ods_battle_log ...")

    # 显式定义 Schema，避免 inferSchema 类型不稳定
    schema = StructType([
        StructField("user_id",            IntegerType(), True),
        StructField("map_id",             IntegerType(), True),
        StructField("battle_result",      IntegerType(), True),
        StructField("battle_duration_s",  FloatType(),   True),
        StructField("base_hp_ratio",      FloatType(),   True),
        StructField("used_special_tower", IntegerType(), True),
        StructField("battle_start_time",  StringType(),  True),   # 先读 String，ETL 再解析
        StructField("session_gap_s",      FloatType(),   True),
        StructField("cumulative_battles", IntegerType(), True),
        StructField("consecutive_fail",   IntegerType(), True),
        StructField("difficulty_tier",    StringType(),  True),
        StructField("wave_count",         IntegerType(), True),
        StructField("tower_score",        FloatType(),   True),
    ])

    df = spark.read.csv(
        f"{HDFS_LANDING}/battle_log.csv",
        schema=schema, header=True
    )

    # 基础校验
    total = df.count()
    null_users = df.filter(F.col("user_id").isNull()).count()
    print(f"  总行数: {total:,}   user_id 为空: {null_users}")
    if null_users > 0:
        print("  ⚠️  过滤掉 user_id 为空的行")
        df = df.filter(F.col("user_id").isNotNull())

    # 写入 ODS（按 RUN_DATE 分区）
    df.withColumn("dt", F.lit(RUN_DATE)) \
      .write \
      .mode("overwrite") \
      .format("orc") \
      .partitionBy("dt") \
      .saveAsTable(f"{HIVE_DB}.ods_battle_log")

    print(f"  写出分区: dt={RUN_DATE}   行数: {df.count():,}")


# ── ODS: map_meta ──
def load_ods_map_meta():
    print("\n[ODS] 加载 map_meta → ods_map_meta ...")

    schema = StructType([
        StructField("map_id",                 IntegerType(), True),
        StructField("map_avg_duration_s",     FloatType(),   True),
        StructField("map_clear_rate",         FloatType(),   True),
        StructField("map_avg_win_duration_s", FloatType(),   True),
        StructField("map_avg_retry_times",    FloatType(),   True),
        StructField("difficulty_tier",        StringType(),  True),
        StructField("map_design_waves",       IntegerType(), True),
    ])

    df = spark.read.csv(
        f"{HDFS_LANDING}/map_meta.csv",
        schema=schema, header=True
    )

    print(f"  地图数: {df.count()}")
    df.write \
      .mode("overwrite") \
      .format("orc") \
      .saveAsTable(f"{HIVE_DB}.ods_map_meta")


# ── ODS: user_label（合并 train/dev/test）──
def load_ods_user_label():
    print("\n[ODS] 加载 train/dev/test → ods_user_label ...")

    train = spark.read.csv(f"{HDFS_LANDING}/train.csv", header=True, inferSchema=True) \
                 .withColumn("split", F.lit("train"))
    dev   = spark.read.csv(f"{HDFS_LANDING}/dev.csv",   header=True, inferSchema=True) \
                 .withColumn("split", F.lit("dev"))
    test  = spark.read.csv(f"{HDFS_LANDING}/test.csv",  header=True, inferSchema=True) \
                 .withColumn("label", F.lit(None).cast(IntegerType())) \
                 .withColumn("split", F.lit("test"))

    # 统一列顺序
    all_labels = train.select("user_id","label","split") \
        .unionByName(dev.select("user_id","label","split")) \
        .unionByName(test.select("user_id","label","split"))

    print(f"  总用户数: {all_labels.count():,}")
    print("  标签分布:")
    all_labels.groupBy("split","label").count().orderBy("split","label").show()

    all_labels.write \
        .mode("overwrite") \
        .format("orc") \
        .saveAsTable(f"{HIVE_DB}.ods_user_label")


# ══════════════════════════════════════════════════════════════
# Step 3: 验证 ODS 写入结果
# ══════════════════════════════════════════════════════════════
def verify_ods():
    print("\n[VERIFY] ODS 验证 ...")
    for tbl in ["ods_battle_log", "ods_map_meta", "ods_user_label"]:
        cnt = spark.table(f"{HIVE_DB}.{tbl}").count()
        print(f"  {tbl:<25} 行数: {cnt:,}")

    # 验证 battle_log 时间戳格式是否正确
    sample = spark.table(f"{HIVE_DB}.ods_battle_log") \
                  .select("battle_start_time").limit(3).collect()
    print(f"\n  battle_start_time 样例（格式应为 yyyy-MM-dd HH:mm:ss）:")
    for row in sample:
        print(f"    {row['battle_start_time']}")

    # 验证 map_meta 难度分布
    print("\n  地图难度分布:")
    spark.table(f"{HIVE_DB}.ods_map_meta") \
         .groupBy("difficulty_tier").count().orderBy("difficulty_tier").show()


# ══════════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════════
def main():
    upload_to_hdfs()
    load_ods_battle_log()
    load_ods_map_meta()
    load_ods_user_label()
    verify_ods()
    spark.stop()
    print(f"\n✅ ODS 入仓完成  分区日期: {RUN_DATE}")


if __name__ == "__main__":
    main()
