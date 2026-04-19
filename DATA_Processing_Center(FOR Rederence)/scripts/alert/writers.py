"""
alert.writers
=============
Persist alert results and daily summary to CSV, Hive, and MySQL.
Also compute map-churn hotspot aggregation.
"""

import os
import pandas as pd

from pyspark.sql import functions as F

from config_env import env
from common.spark_helper import mysql_exec_sql, write_mysql_idempotent

PROJECT_DIR = env("TD_CHURN_PROJECT_DIR", "/home/hadoop/td_churn_project")
OUTPUT_DIR  = env("TD_CHURN_ALERT_OUTPUT_DIR", f"{PROJECT_DIR}/alert_output")
HIVE_DB     = env("TD_CHURN_HIVE_DB", "td_churn")
# FK Do not insert “；”，only “&”，REMENMBER IT！！》:(
MYSQL_URL   = env("TD_CHURN_DB_URL", required=True).replace("&amp;", "&").replace(";", "&")
MYSQL_USER  = env("TD_CHURN_DB_USER", required=True)
MYSQL_PASS  = env("TD_CHURN_DB_PASSWORD", required=True)

os.makedirs(OUTPUT_DIR, exist_ok=True)


def _ensure_hive_tables(spark):
    """Create ADS Hive tables if they don't exist (idempotent)."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {HIVE_DB}.ads_user_churn_risk (
            user_id INT, churn_prob FLOAT, risk_level STRING,
            rule_trigger STRING, final_alert TINYINT,
            top_reason_1 STRING, top_reason_2 STRING, top_reason_3 STRING,
            predict_dt STRING, model_version STRING
        ) PARTITIONED BY (dt STRING) STORED AS ORC
          TBLPROPERTIES ('orc.compress'='SNAPPY')
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {HIVE_DB}.ads_daily_churn_summary (
            stat_date STRING, total_active_users INT,
            high_risk_count INT, medium_risk_count INT, low_risk_count INT,
            high_risk_rate FLOAT, avg_churn_prob FLOAT,
            top_stuck_map_id INT, d1_no_tutorial_count INT,
            avg_battles_per_user FLOAT, stuck_user_count INT,
            narrow_win_rate_overall FLOAT, top_stuck_map_id_2 INT
        ) STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY')
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {HIVE_DB}.ads_map_churn_hotspot (
            map_id INT, difficulty_tier STRING,
            total_attempts INT, fail_rate FLOAT,
            avg_hp_ratio FLOAT, help_usage_rate FLOAT,
            player_count INT, high_risk_player_count INT,
            map_clear_rate FLOAT, stat_date STRING
        ) PARTITIONED BY (dt STRING) STORED AS ORC
          TBLPROPERTIES ('orc.compress'='SNAPPY')
    """)


def write_alert_result(spark, result_df, result_sdf, run_dt):
    """Write per-user alert results to CSV, Hive, and MySQL."""
    _ensure_hive_tables(spark)

    # ── Derive per-user dt from last active battle date ──
    try:
        user_last_dt = spark.table(f"{HIVE_DB}.dwd_battle_detail") \
            .groupBy("user_id").agg(F.max("dt").alias("dt"))
        result_sdf = result_sdf.join(user_last_dt, on="user_id", how="left") \
            .fillna({"dt": run_dt})
        dt_pd = user_last_dt.toPandas().set_index("user_id")["dt"]
        result_df["dt"] = result_df["user_id"].map(dt_pd).fillna(run_dt)
    except Exception as e:
        print(f"  [WARN] Could not derive user dt, using run_dt: {e}")
        result_sdf = result_sdf.withColumn("dt", F.lit(run_dt))
        result_df["dt"] = run_dt

    # CSV
    result_df.to_csv(f"{OUTPUT_DIR}/alert_result.csv", index=False,
                     encoding="utf-8-sig")
    print(f"\n  CSV saved: {OUTPUT_DIR}/alert_result.csv")

    # Hive (dynamic partition by dt)
    try:
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        result_sdf.createOrReplaceTempView("tmp_ads_user_churn_risk_v2")
        spark.sql(f"""
            INSERT OVERWRITE TABLE {HIVE_DB}.ads_user_churn_risk PARTITION (dt)
            SELECT
                CAST(user_id AS INT)            AS user_id,
                CAST(churn_prob AS FLOAT)       AS churn_prob,
                CAST(risk_level AS STRING)      AS risk_level,
                CAST(rule_trigger AS STRING)    AS rule_trigger,
                CAST(final_alert AS TINYINT)    AS final_alert,
                CAST(top_reason_1 AS STRING)    AS top_reason_1,
                CAST(top_reason_2 AS STRING)    AS top_reason_2,
                CAST(top_reason_3 AS STRING)    AS top_reason_3,
                CAST(predict_dt AS STRING)      AS predict_dt,
                CAST(model_version AS STRING)   AS model_version,
                dt
            FROM tmp_ads_user_churn_risk_v2
        """)
        print(f"  Hive -> {HIVE_DB}.ads_user_churn_risk (dynamic partition) OK")
    except Exception as e:
        print(f"  [WARN] Hive ads_user_churn_risk skipped: {e}")

    # MySQL
    try:
        mysql_exec_sql(spark, MYSQL_URL, MYSQL_USER, MYSQL_PASS,
                       "DELETE FROM ads_user_churn_risk;")
        result_sdf.write.format("jdbc") \
            .option("url", MYSQL_URL).option("dbtable", "ads_user_churn_risk") \
            .option("user", MYSQL_USER).option("password", MYSQL_PASS) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("append").save()
        print("  MySQL -> ads_user_churn_risk OK")
    except Exception as e:
        print(f"  [WARN] MySQL ads_user_churn_risk skipped: {e}")


def write_daily_summary(spark, result_df, feature_df, run_dt):
    """Compute and write daily churn summary to CSV, Hive, and MySQL."""
    # Compute summary fields
    d1_no_tutorial_count = 0
    if "d1_completed_map1" in feature_df.columns:
        d1_no_tutorial_count = int((feature_df["d1_completed_map1"].fillna(0) == 0).sum())

    top_stuck_map_id = -1
    top_stuck_map_id_2 = -1
    if "first_stuck_map_id" in feature_df.columns:
        tmp = feature_df["first_stuck_map_id"].dropna()
        try:
            tmp = tmp.astype(int)
            tmp = tmp[tmp > 0]
        except Exception:
            pass
        if len(tmp) > 0:
            vc = tmp.value_counts()
            top_stuck_map_id = int(vc.index[0])
            if len(vc) >= 2:
                top_stuck_map_id_2 = int(vc.index[1])

    avg_battles = float(round(feature_df["total_battles"].mean(), 2)) \
        if "total_battles" in feature_df.columns else 0.0
    stuck_count = int((feature_df["stuck_level_count"] > 0).sum()) \
        if "stuck_level_count" in feature_df.columns else 0
    narrow_wr = float(round(feature_df["narrow_win_rate"].mean(), 4)) \
        if "narrow_win_rate" in feature_df.columns else 0.0

    # Use actual data date (max battle date) instead of pipeline run date
    try:
        max_dt = spark.table(f"{HIVE_DB}.dwd_battle_detail") \
            .agg(F.max("dt")).collect()[0][0]
        stat_date = max_dt if max_dt else run_dt
    except Exception:
        stat_date = run_dt

    summary = {
        "stat_date":              stat_date,
        "total_active_users":     int(len(result_df)),
        "high_risk_count":        int((result_df["risk_level"] == "high").sum()),
        "medium_risk_count":      int((result_df["risk_level"] == "medium").sum()),
        "low_risk_count":         int((result_df["risk_level"] == "low").sum()),
        "high_risk_rate":         float(round((result_df["risk_level"] == "high").mean(), 4)),
        "avg_churn_prob":         float(round(result_df["churn_prob"].mean(), 4)),
        "top_stuck_map_id":       int(top_stuck_map_id),
        "d1_no_tutorial_count":   int(d1_no_tutorial_count),
        "avg_battles_per_user":   avg_battles,
        "stuck_user_count":       stuck_count,
        "narrow_win_rate_overall": narrow_wr,
        "top_stuck_map_id_2":     int(top_stuck_map_id_2),
    }

    # CSV
    pd.DataFrame([summary]).to_csv(f"{OUTPUT_DIR}/daily_summary.csv",
                                   index=False, encoding="utf-8-sig")
    print(f"  Daily summary CSV saved: {OUTPUT_DIR}/daily_summary.csv")

    # Hive
    _SUMMARY_SQL = """
        SELECT
            CAST(stat_date AS STRING)              AS stat_date,
            CAST(total_active_users AS INT)        AS total_active_users,
            CAST(high_risk_count AS INT)           AS high_risk_count,
            CAST(medium_risk_count AS INT)         AS medium_risk_count,
            CAST(low_risk_count AS INT)            AS low_risk_count,
            CAST(high_risk_rate AS FLOAT)          AS high_risk_rate,
            CAST(avg_churn_prob AS FLOAT)          AS avg_churn_prob,
            CAST(top_stuck_map_id AS INT)          AS top_stuck_map_id,
            CAST(d1_no_tutorial_count AS INT)      AS d1_no_tutorial_count,
            CAST(avg_battles_per_user AS FLOAT)    AS avg_battles_per_user,
            CAST(stuck_user_count AS INT)          AS stuck_user_count,
            CAST(narrow_win_rate_overall AS FLOAT) AS narrow_win_rate_overall,
            CAST(top_stuck_map_id_2 AS INT)        AS top_stuck_map_id_2
        FROM tmp_ads_daily_churn_summary_v2
    """
    try:
        summary_sdf = spark.createDataFrame([summary])
        summary_sdf.createOrReplaceTempView("tmp_ads_daily_churn_summary_v2")
        try:
            spark.sql(f"""
                INSERT OVERWRITE TABLE {HIVE_DB}.ads_daily_churn_summary
                PARTITION (dt='{run_dt}') {_SUMMARY_SQL}
            """)
            print(f"  Hive -> {HIVE_DB}.ads_daily_churn_summary dt={run_dt} OK")
        except Exception:
            spark.sql(f"""
                INSERT OVERWRITE TABLE {HIVE_DB}.ads_daily_churn_summary
                {_SUMMARY_SQL}
            """)
            print(f"  Hive -> {HIVE_DB}.ads_daily_churn_summary (non-partitioned) OK")
    except Exception as e:
        print(f"  [WARN] Hive ads_daily_churn_summary skipped: {e}")

    # MySQL
    try:
        mysql_exec_sql(spark, MYSQL_URL, MYSQL_USER, MYSQL_PASS,
                       f"DELETE FROM ads_daily_churn_summary WHERE stat_date = '{stat_date}';")
        spark.createDataFrame([summary]).write.format("jdbc") \
            .option("url", MYSQL_URL).option("dbtable", "ads_daily_churn_summary") \
            .option("user", MYSQL_USER).option("password", MYSQL_PASS) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("append").save()
        print("  MySQL -> ads_daily_churn_summary OK")
    except Exception as e:
        print(f"  [WARN] MySQL ads_daily_churn_summary skipped: {e}")


def write_map_hotspot(spark, result_sdf, run_dt):
    """Aggregate map-level churn hotspot from DWD and write to Hive + MySQL."""
    try:
        dwd = spark.table(f"{HIVE_DB}.dwd_battle_detail")
        # dwd_battle_detail has difficulty_num (INT 1-4), derive difficulty_tier (STRING)
        dwd = dwd.withColumn("difficulty_tier", F.when(F.col("difficulty_num") == 1, "easy")
                              .when(F.col("difficulty_num") == 2, "normal")
                              .when(F.col("difficulty_num") == 3, "hard")
                              .when(F.col("difficulty_num") == 4, "expert")
                              .otherwise("unknown"))
        hotspot = dwd.groupBy("map_id", "difficulty_tier").agg(
            F.count("*").alias("total_attempts"),
            (F.lit(1) - F.mean("battle_result")).alias("fail_rate"),
            F.mean("base_hp_ratio").alias("avg_hp_ratio"),
            F.mean("used_special_tower").alias("help_usage_rate"),
            F.countDistinct("user_id").alias("player_count"),
            F.mean("battle_result").cast("float").alias("map_clear_rate"),
        )
        high_uids = result_sdf.filter(F.col("risk_level") == "high").select("user_id")
        hr_per_map = (
            dwd.join(high_uids, on="user_id", how="inner")
               .groupBy("map_id")
               .agg(F.countDistinct("user_id").alias("high_risk_player_count"))
        )
        hotspot = (
            hotspot.join(hr_per_map, on="map_id", how="left")
            .fillna({"high_risk_player_count": 0})
            .withColumn("stat_date", F.lit(run_dt))
        )

        # Hive
        try:
            hotspot.createOrReplaceTempView("tmp_ads_map_churn_hotspot")
            spark.sql(f"""
                INSERT OVERWRITE TABLE {HIVE_DB}.ads_map_churn_hotspot
                PARTITION (dt='{run_dt}')
                SELECT map_id, difficulty_tier,
                       CAST(total_attempts AS INT),
                       CAST(fail_rate AS FLOAT),
                       CAST(avg_hp_ratio AS FLOAT),
                       CAST(help_usage_rate AS FLOAT),
                       CAST(player_count AS INT),
                       CAST(high_risk_player_count AS INT),
                       CAST(map_clear_rate AS FLOAT),
                       stat_date
                FROM tmp_ads_map_churn_hotspot
            """)
            print(f"  Hive -> {HIVE_DB}.ads_map_churn_hotspot dt={run_dt} OK")
        except Exception as e:
            print(f"  [WARN] Hive ads_map_churn_hotspot skipped: {e}")

        # MySQL
        try:
            mysql_exec_sql(spark, MYSQL_URL, MYSQL_USER, MYSQL_PASS,
                           f"DELETE FROM ads_map_churn_hotspot WHERE stat_date = '{run_dt}';")
            hotspot.write.format("jdbc") \
                .option("url", MYSQL_URL).option("dbtable", "ads_map_churn_hotspot") \
                .option("user", MYSQL_USER).option("password", MYSQL_PASS) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .mode("append").save()
            print("  MySQL -> ads_map_churn_hotspot OK")
        except Exception as e:
            print(f"  [WARN] MySQL ads_map_churn_hotspot skipped: {e}")

    except Exception as e:
        print(f"  [WARN] Map hotspot aggregation skipped: {e}")
