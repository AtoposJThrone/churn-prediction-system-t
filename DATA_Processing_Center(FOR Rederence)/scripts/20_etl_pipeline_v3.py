"""
etl_pipeline_v2.py  （修复版）
================================
对照原版本的全部 Bug 修复清单
-------------------------------
Bug-1  stuck_group_reset 第一行 NULL 导致分组起始错误
         → 增加 prev_map.isNull() 判断，确保每位用户第一条记录开启新分组

Bug-2  fail_in_group 用 count(*) 把胜利场也计入连败数
         → 改用 sum(when(result==0,1)) 只累计失败场

Bug-3  DWD 分区表写入缺少 dt 列，insertInto 报错
         → 写入前加 dt 列，改用 .write.partitionBy("dt").saveAsTable()

Bug-4  ADS 层错误 insertInto：result 特征表写入 ads_user_churn_risk
         Schema 完全不匹配
         → 删除该写入；ADS 由 rule_engine.py 负责填充

Bug-5  quality_check 对每列单独 .count() 触发 N 次全扫，极慢
         → 改为单次 select(sum(isNull)) 聚合完成所有列

Bug-6  dws_user_battle_stats insertInto 时 hard_map_battles
         列在 Hive DDL 中不存在，导致列数不匹配
         → 写 Hive 前显式 select 去掉该列；Parquet 输出保留

Bug-7  insertInto 按位置匹配列，DataFrame 列顺序不一定与 DDL 一致
         → 所有 Hive 写入前加显式按名 .select(顺序列表)

Bug-8  feat_time 链式 join 中 total_battles 临时列未及时 drop，
         与 feat_battle 重名导致后续 join 歧义
         → 拆分为独立步骤，join 后立即 drop

Bug-9  d1_sessions 过滤用 session_df.day_index，但 session 可能
         跨午夜，导致部分首日 session 被漏计
         → 改用 session 内最小 battle_start_ts 所在 day 做判断
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, StringType

import os
from datetime import datetime

from config_env import env, env_bool, env_int

# ─────────────────────── 配置 ──────────────────────────────────
PROJECT_DIR    = env("TD_CHURN_PROJECT_DIR", "/home/hadoop/td_churn_project")
HIVE_DB        = env("TD_CHURN_HIVE_DB", "td_churn")
USE_HIVE       = env_bool("TD_CHURN_USE_HIVE", True)
DATA_DIR       = env("TD_CHURN_TRANSFORMED_DIR", "/DataSet_Transformed")
OUTPUT_DIR     = env("TD_CHURN_ETL_OUTPUT_DIR", f"{PROJECT_DIR}/etl_output_v2")

MYSQL_URL      = env("TD_CHURN_DB_URL", required=True)
MYSQL_USER     = env("TD_CHURN_DB_USER", required=True)
MYSQL_PASSWORD = env("TD_CHURN_DB_PASSWORD", required=True)
MYSQL_DRIVER   = "com.mysql.cj.jdbc.Driver"
WAREHOUSE_DIR  = env("TD_CHURN_HIVE_WAREHOUSE_DIR", "/user/hive/warehouse")
SHUFFLE_PARTITIONS = str(env_int("TD_CHURN_SPARK_SHUFFLE_PARTITIONS", 200))

SESSION_GAP_THRESHOLD = 1800
TUTORIAL_MAP_ID       = 1
STUCK_FAIL_THRESHOLD  = 2
RUN_DATE = datetime.today().strftime("%Y-%m-%d")

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ─────────────────────── Spark 初始化 ──────────────────────────
def build_spark():
    builder = SparkSession.builder \
        .appName("TowerDefense_Churn_ETL_v2") \
        .config("spark.sql.shuffle.partitions",                  SHUFFLE_PARTITIONS) \
        .config("spark.sql.adaptive.enabled",                    "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.orc.impl",                            "native")

    if USE_HIVE:
        builder = builder \
            .enableHiveSupport() \
            .config("spark.sql.warehouse.dir",          WAREHOUSE_DIR) \
            .config("hive.exec.dynamic.partition",      "true") \
            .config("hive.exec.dynamic.partition.mode", "nonstrict")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if USE_HIVE:
        spark.sql(f"USE {HIVE_DB}")
    return spark


# ══════════════════════════════════════════════════════════════
# E x t r a c t
# ══════════════════════════════════════════════════════════════
def extract(spark):
    print("\n[EXTRACT] 读取 ODS 层数据 ...")

    if USE_HIVE:
        battle_log  = spark.table(f"{HIVE_DB}.ods_battle_log")
        map_meta    = spark.table(f"{HIVE_DB}.ods_map_meta")
        user_labels = spark.table(f"{HIVE_DB}.ods_user_label")
        if "dt" in battle_log.columns:
            battle_log = battle_log.drop("dt")
    else:
        battle_log  = spark.read.csv(f"{DATA_DIR}/battle_log.csv",
                                     header=True, inferSchema=True)
        map_meta    = spark.read.csv(f"{DATA_DIR}/map_meta.csv",
                                     header=True, inferSchema=True)
        train = spark.read.csv(f"{DATA_DIR}/train.csv", header=True, inferSchema=True) \
                     .withColumn("split", F.lit("train"))
        dev   = spark.read.csv(f"{DATA_DIR}/dev.csv",   header=True, inferSchema=True) \
                     .withColumn("split", F.lit("dev"))
        test  = spark.read.csv(f"{DATA_DIR}/test.csv",  header=True, inferSchema=True) \
                     .withColumn("label", F.lit(None).cast(IntegerType())) \
                     .withColumn("split", F.lit("test"))
        user_labels = train.unionByName(dev).unionByName(test)

    battle_log = battle_log \
        .withColumn("battle_start_ts",
                    F.unix_timestamp("battle_start_time", "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("battle_date", F.to_date("battle_start_time"))

    battle_log.cache()
    print(f"  battle_log  : {battle_log.count():,} 行")
    print(f"  user_labels : {user_labels.count():,} 用户")
    return battle_log, map_meta, user_labels


# ══════════════════════════════════════════════════════════════
# T r a n s f o r m  Step-1 ── DWD
# ══════════════════════════════════════════════════════════════
def build_dwd(spark, battle_log, map_meta):
    print("\n[DWD] 清洗 & 明细层构建 ...")

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

    # 字段兼容处理：当 ODS 来自旧版数据（未运行新版 11_data_transform.py）时自动补齐默认値
    for _col, _default in [("is_narrow_win", 0), ("is_first_attempt", 0)]:
        if _col not in bl.columns:
            bl = bl.withColumn(_col, F.lit(_default))

    # ── 卡关检测（Bug-1 & Bug-2 双修）──
    w_time = Window.partitionBy("user_id").orderBy("battle_start_ts")

    bl = bl \
        .withColumn("prev_map", F.lag("map_id", 1).over(w_time)) \
        .withColumn(
            "stuck_group_reset",
            # Bug-1：prev_map.isNull() 为第一行，必须开启新分组
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
            # Bug-2：只对失败场累计，胜利场计 0
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

    # ── Session 切分 ──
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

    # ── day_index（Bug-8：拆分 join+withColumn，避免列歧义）──
    user_first_day = bl.groupBy("user_id") \
                       .agg(F.min("battle_date").alias("reg_date"))
    bl = bl.join(user_first_day, on="user_id", how="left") \
           .withColumn("day_index", F.datediff("battle_date", "reg_date") + 1) \
           .withColumn("is_day1",   F.when(F.col("day_index") == 1, 1).otherwise(0))

    # 清理辅助列
    bl = bl.drop("prev_map", "stuck_group_reset", "stuck_group_id",
                 "fail_in_group", "is_new_session", "session_seq", "reg_date")
    bl.cache()
    print(f"  清洗后行数: {bl.count():,}")

    # ── Bug-3：写分区 DWD 表（加 dt，用 saveAsTable+partitionBy）──
    if USE_HIVE:
        dwd_cols = [
            "user_id", "map_id", "battle_result", "battle_duration_s",
            "base_hp_ratio", "used_special_tower", "battle_start_time",
            "battle_start_ts", "battle_date", "session_gap_s", "session_id",
            "cumulative_battles", "consecutive_fail", "is_stuck",
            "day_index", "is_day1", "wave_count", "tower_score",
            "difficulty_num", "map_clear_rate", "map_avg_retry_times",
            "is_narrow_win", "is_first_attempt",  # 新增业务标记字段
        ]
        bl.select([c for c in dwd_cols if c in bl.columns]) \
          .withColumn("dt", F.lit(RUN_DATE)) \
          .write.mode("overwrite").format("orc") \
          .partitionBy("dt") \
          .saveAsTable(f"{HIVE_DB}.dwd_battle_detail")
    else:
        bl.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/dwd_battle_detail")

    # ── Session 明细（Bug-9：以 session 内最早 day_index 为基准）──
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
        session_df.withColumn("dt", F.lit(RUN_DATE)) \
                  .write.mode("overwrite").format("orc") \
                  .partitionBy("dt") \
                  .saveAsTable(f"{HIVE_DB}.dwd_session_detail")
    else:
        session_df.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/dwd_session_detail")

    return bl, session_df


# ══════════════════════════════════════════════════════════════
# T r a n s f o r m  Step-2 ── DWS
# ══════════════════════════════════════════════════════════════
def build_dws(spark, bl, session_df):
    print("\n[DWS] 特征聚合 ...")

    # A. 全周期战斗统计
    feat_battle = bl.groupBy("user_id").agg(
        F.count("*")                                        .alias("total_battles"),
        F.mean("battle_result")                             .alias("overall_win_rate"),
        F.mean("battle_duration_s")                         .alias("avg_battle_duration"),
        F.stddev("battle_duration_s")                       .alias("std_battle_duration"),
        F.mean("base_hp_ratio")                             .alias("avg_base_hp_ratio"),
        F.mean(F.when(F.col("battle_result")==1,
                      F.col("base_hp_ratio")))              .alias("avg_win_base_hp"),
        F.sum("used_special_tower")                         .alias("total_special_tower_used"),
        F.mean("used_special_tower")                        .alias("special_tower_use_rate"),
        F.mean("tower_score")                               .alias("avg_tower_score"),
        F.max("tower_score")                                .alias("max_tower_score"),
        F.sum("wave_count")                                 .alias("total_waves_survived"),
    )
    feat_battle.cache()

    # B. 关卡进度
    feat_progress = bl.groupBy("user_id").agg(
        F.max("map_id")                                     .alias("max_map_reached"),
        F.countDistinct("map_id")                           .alias("unique_maps_played"),
        F.mean("difficulty_num")                            .alias("avg_difficulty_played"),
        F.max("difficulty_num")                             .alias("max_difficulty_reached"),
        F.mean(F.when(F.col("difficulty_num") >= 3, 1)
                .otherwise(0))                              .alias("hard_map_ratio"),
    )

    # C. 卡关特征
    stuck_bl = bl.filter(F.col("is_stuck") == 1)
    feat_stuck = stuck_bl.groupBy("user_id").agg(
        F.countDistinct("map_id").alias("stuck_level_count"),
        F.count("*")             .alias("stuck_total_attempts"),
    )
    first_stuck = stuck_bl \
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("user_id").orderBy("battle_start_ts")
        )) \
        .filter(F.col("rn") == 1) \
        .select("user_id", F.col("map_id").alias("first_stuck_map_id"))
    feat_stuck = feat_stuck.join(first_stuck, on="user_id", how="left")

    # D. 道具依赖（Bug-6：hard_map_battles 仅留 Parquet，Hive 写入时 drop）
    feat_item_hard = bl.filter(F.col("difficulty_num") >= 3).groupBy("user_id").agg(
        F.mean("used_special_tower").alias("hard_map_special_rate"),
        F.count("*")               .alias("hard_map_battles"),
    )
    feat_item_fail = bl.filter(F.col("battle_result") == 0).groupBy("user_id").agg(
        F.mean("used_special_tower").alias("fail_special_rate"),
    )

    # E. 时序 & 会话（Bug-8：拆分消除 total_battles 列歧义）
    feat_session = session_df.groupBy("user_id").agg(
        F.count("*")                 .alias("total_sessions"),
        F.mean("session_battles")    .alias("avg_battles_per_session"),
        F.mean("session_duration_s") .alias("avg_session_duration_s"),
    )
    feat_time = bl.groupBy("user_id").agg(
        F.countDistinct("battle_date").alias("active_days"),
        F.min("battle_date")          .alias("_first_dt"),
        F.max("battle_date")          .alias("_last_dt"),
        F.mean("session_gap_s")       .alias("avg_session_gap_s"),
        F.max("session_gap_s")        .alias("max_session_gap_s"),
    ) \
    .withColumn("observation_span_days", F.datediff("_last_dt", "_first_dt") + 1) \
    .drop("_first_dt", "_last_dt") \
    .join(feat_battle.select("user_id", "total_battles"), on="user_id", how="left") \
    .withColumn("battles_per_active_day",
                F.col("total_battles") / F.greatest(F.col("active_days"), F.lit(1))) \
    .drop("total_battles")

    # F. 连败压力
    feat_streak = bl.groupBy("user_id").agg(
        F.max("consecutive_fail")                        .alias("max_consecutive_fail"),
        F.mean("consecutive_fail")                       .alias("avg_consecutive_fail"),
        F.mean(F.when(F.col("consecutive_fail") >= 3, 1)
                .otherwise(0))                           .alias("severe_fail_streak_ratio"),
    )

    # G. 近期趋势（最近10场）
    feat_recent = bl \
        .withColumn("rk", F.row_number().over(
            Window.partitionBy("user_id").orderBy(F.desc("battle_start_ts"))
        )) \
        .filter(F.col("rk") <= 10) \
        .groupBy("user_id").agg(
            F.mean("battle_result") .alias("recent10_win_rate"),
            F.mean("tower_score")   .alias("recent10_avg_score"),
            F.mean("difficulty_num").alias("recent10_avg_difficulty"),
        )

    # H. 地图元数据聚合
    feat_meta = bl.groupBy("user_id").agg(
        F.mean("map_clear_rate")      .alias("avg_map_clear_rate_played"),
        F.min("map_clear_rate")       .alias("min_map_clear_rate_played"),
        F.mean("map_avg_retry_times") .alias("avg_map_retry_rate_played"),
    )

    # I. 新增业务洞察特征
    # 险胜率：胜利但基地血量极低（<20%），说明玩家在高压局面下仍能获胜，可能具备较强的适应能力和抗压性
    feat_insights = bl.groupBy("user_id").agg(
        F.sum(F.col("is_narrow_win").cast("int")) .alias("narrow_win_count"),
        F.mean(F.col("is_narrow_win").cast("double")) .alias("narrow_win_rate"),
    )

    # 首次尝试胜率：只统计每个地图第一次挑战的结果，衡量初始难度适应性
    feat_first_attempt = (
        bl.filter(F.col("is_first_attempt") == 1)
        .groupBy("user_id")
        .agg(F.mean("battle_result").alias("first_attempt_win_rate"))
    )

    # 重试地图数：有过第2次及以上尝试的地图数，让人看看狗策划做的什么垃圾关卡（生气脸）
    feat_retry = (
        bl.filter(F.col("is_first_attempt") == 0)
        .groupBy("user_id")
        .agg(F.countDistinct("map_id").alias("retry_map_count"))
    )

    # 汇总全周期
    dws_all = feat_battle \
        .join(feat_progress,      on="user_id", how="left") \
        .join(feat_stuck,         on="user_id", how="left") \
        .join(feat_item_hard,     on="user_id", how="left") \
        .join(feat_item_fail,     on="user_id", how="left") \
        .join(feat_session,       on="user_id", how="left") \
        .join(feat_time,          on="user_id", how="left") \
        .join(feat_streak,        on="user_id", how="left") \
        .join(feat_recent,        on="user_id", how="left") \
        .join(feat_meta,          on="user_id", how="left") \
        .join(feat_insights,      on="user_id", how="left") \
        .join(feat_first_attempt, on="user_id", how="left") \
        .join(feat_retry,         on="user_id", how="left") \
        .fillna(0, subset=[
            "stuck_level_count", "stuck_total_attempts",
            "hard_map_special_rate", "hard_map_battles",
            "fail_special_rate", "severe_fail_streak_ratio",
            "narrow_win_count", "narrow_win_rate",
            "first_attempt_win_rate", "retry_map_count",
        ])

    # 关卡推进速率：最高关卡编号 / 观测天数，表示用户关卡推进节奏的快慢
    dws_all = dws_all.withColumn(
        "progress_velocity",
        F.col("max_map_reached").cast("double") /
        F.greatest(F.col("observation_span_days"), F.lit(1)).cast("double")
    )

    # I. 时间窗口 D1 / D3 / D7
    def window_stats(prefix, max_day):
        wdf = bl.filter(F.col("day_index") <= max_day)
        base = wdf.groupBy("user_id").agg(
            F.count("*")                  .alias(f"{prefix}_battles"),
            F.mean("battle_result")       .alias(f"{prefix}_win_rate"),
            F.max("map_id")               .alias(f"{prefix}_max_map"),
            F.countDistinct("battle_date").alias(f"{prefix}_active_days"),
            F.sum("is_stuck")             .alias(f"{prefix}_stuck_count"),
            F.mean("used_special_tower")  .alias(f"{prefix}_special_rate"),
        )
        return base.withColumn(
            f"{prefix}_avg_daily_battles",
            F.col(f"{prefix}_battles") /
            F.greatest(F.col(f"{prefix}_active_days"), F.lit(1))
        )

    feat_d1_base = bl.filter(F.col("day_index") == 1).groupBy("user_id").agg(
        F.count("*")                                 .alias("d1_battles"),
        F.mean("battle_result")                      .alias("d1_win_rate"),
        F.max("map_id")                              .alias("d1_max_map"),
        F.mean("used_special_tower")                 .alias("d1_special_rate"),
        F.sum(F.col("battle_duration_s") / 60)       .alias("d1_active_minutes"),
        F.max(F.when(F.col("map_id") == TUTORIAL_MAP_ID,
                     F.col("battle_result")))        .alias("d1_completed_map1"),
        F.countDistinct("map_id")                    .alias("d1_unique_maps"),  # new：首日探索关卡数
    )
    # Bug-9：以 session 内最小 day_index 判断首日 session
    d1_sessions = session_df.filter(F.col("day_index") == 1) \
                            .groupBy("user_id") \
                            .agg(F.count("*").alias("d1_sessions"))
    feat_d1 = feat_d1_base \
        .join(d1_sessions, on="user_id", how="left") \
        .fillna({"d1_sessions": 0, "d1_completed_map1": 0})

    feat_d3 = window_stats("d3", 3)
    feat_d7 = window_stats("d7", 7)

    print(f"  DWS 全周期特征列数: {len(dws_all.columns)}")

    # 缓存 DWS 主表并强制物化
    dws_all.cache()
    dws_all.count()      # 触发物化，后续 join 和写入直接使用缓存

    # ── Hive 写入（Bug-6 & Bug-7：显式 select，drop 多余列）──
    if USE_HIVE:
        # dws_user_battle_stats（严格按 DDL 列序）
        dws_hive_cols = [
            "user_id",
            "total_battles", "overall_win_rate", "avg_battle_duration",
            "std_battle_duration", "avg_base_hp_ratio", "avg_win_base_hp",
            "total_special_tower_used", "special_tower_use_rate",
            "avg_tower_score", "max_tower_score", "total_waves_survived",
            "max_map_reached", "unique_maps_played", "avg_difficulty_played",
            "max_difficulty_reached", "hard_map_ratio",
            "stuck_level_count", "stuck_total_attempts", "first_stuck_map_id",
            "hard_map_special_rate",            # hard_map_battles 不在 DDL 中，skip
            "fail_special_rate",
            "active_days", "total_sessions", "avg_battles_per_session",
            "avg_session_duration_s", "avg_session_gap_s", "max_session_gap_s",
            "battles_per_active_day", "observation_span_days",
            "max_consecutive_fail", "avg_consecutive_fail", "severe_fail_streak_ratio",
            "recent10_win_rate", "recent10_avg_score", "recent10_avg_difficulty",
            "avg_map_clear_rate_played", "min_map_clear_rate_played",
            "avg_map_retry_rate_played",
            # 新增业务洞察特征
            "narrow_win_count", "narrow_win_rate", "first_attempt_win_rate",
            "retry_map_count", "progress_velocity",
        ] # 注意顺序必须与 Hive DDL 完全一致
        dws_all.select(dws_hive_cols) \
               .write.mode("overwrite").format("orc") \
               .saveAsTable(f"{HIVE_DB}.dws_user_battle_stats")

        feat_d1.select(["user_id","d1_battles","d1_win_rate","d1_max_map",
                        "d1_completed_map1","d1_special_rate",
                        "d1_active_minutes","d1_sessions","d1_unique_maps"]) \
               .write.mode("overwrite").format("orc") \
               .saveAsTable(f"{HIVE_DB}.dws_user_d1_stats") # 新增 d1_unique_maps

        feat_d3.select(["user_id","d3_battles","d3_win_rate","d3_max_map",
                        "d3_active_days","d3_avg_daily_battles",
                        "d3_stuck_count","d3_special_rate"]) \
               .write.mode("overwrite").format("orc") \
               .saveAsTable(f"{HIVE_DB}.dws_user_d3_stats")

        feat_d7.select(["user_id","d7_battles","d7_win_rate","d7_max_map",
                        "d7_active_days","d7_avg_daily_battles",
                        "d7_stuck_count","d7_special_rate"]) \
               .write.mode("overwrite").format("orc") \
               .saveAsTable(f"{HIVE_DB}.dws_user_d7_stats")
    else:
        dws_all.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/dws_user_battle_stats")
        feat_d1.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/dws_user_d1_stats")
        feat_d3.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/dws_user_d3_stats")
        feat_d7.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/dws_user_d7_stats")

    return dws_all, feat_d1, feat_d3, feat_d7


# ══════════════════════════════════════════════════════════════
# T r a n s f o r m  Step-3 ── ADS 特征表（附标签）
# ══════════════════════════════════════════════════════════════
def build_ads_features(spark, dws_all, feat_d1, feat_d3, feat_d7, user_labels):
    print("\n[ADS] 拼接标签 ...")

    # If USE Hive then read data form hive to avoid OOM
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

    print(f"  最终列数（含 user_id/label/split）: {len(result.columns)}")
    return result


# ══════════════════════════════════════════════════════════════
# L o a d
# ══════════════════════════════════════════════════════════════
def load(spark, result):
    print("\n[LOAD] 写出 Parquet ...")
    for split in ["train", "dev", "test"]:
        out_path = f"{OUTPUT_DIR}/features_{split}.parquet"
        df_split = result.filter(F.col("split") == split).drop("split")
        df_split.coalesce(4).write.mode("overwrite").parquet(out_path)
        print(f"  {split}: {df_split.count():,} 用户 → {out_path}")

    # Bug-4：不写入 ads_user_churn_risk（由 rule_engine.py 填充）
    # 仅写基础特征摘要到 MySQL 供前端基线展示
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
        print("  MySQL → user_feature_summary 写出成功")
    except Exception as e:
        print(f"  [WARN] MySQL 跳过: {e}")


# ══════════════════════════════════════════════════════════════
# Q u a l i t y  C h e c k（Bug-5：单次扫描）
# ══════════════════════════════════════════════════════════════
def quality_check(result):
    print("\n[QC] 数据质量检查 ...")
    train_df = result.filter(F.col("split") == "train")
    n = train_df.count()

    check_cols = [c for c in train_df.columns
                  if c not in ("user_id", "label", "split")]
    # 单次聚合，不再逐列 count（Bug-5 修复）
    null_row = train_df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in check_cols
    ]).collect()[0].asDict()

    null_rates = sorted(
        [(col, cnt / n) for col, cnt in null_row.items()],
        key=lambda x: -x[1]
    )
    print(f"  训练集: {n:,} 用户")
    print("  NULL 比例 Top10:")
    for col_name, rate in null_rates[:10]:
        flag = "⚠️ " if rate > 0.1 else "✅ "
        print(f"    {flag}{col_name:<45} {rate:.2%}")

    print("\n  标签分布:")
    result.filter(F.col("split") == "train").groupBy("label").count().show()

    print("  关键特征预览（前5行）:")
    result.filter(F.col("split") == "train") \
          .select("user_id", "label", "total_battles", "overall_win_rate",
                  "max_map_reached", "stuck_level_count",
                  "d1_battles", "d1_completed_map1", "max_consecutive_fail") \
          .show(5, truncate=False)


# ══════════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════════
def main():
    spark = build_spark()
    battle_log, map_meta, user_labels = extract(spark)
    bl, session_df               = build_dwd(spark, battle_log, map_meta)
    dws_all, d1, d3, d7          = build_dws(spark, bl, session_df)
    result                       = build_ads_features(spark, dws_all, d1, d3, d7, user_labels)
    load(spark, result)
    quality_check(result)
    spark.stop()
    print(f"\n✅ ETL v2 完成  RUN_DATE={RUN_DATE}")


if __name__ == "__main__":
    main()
