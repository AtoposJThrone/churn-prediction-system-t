"""
etl.dws
=======
DWS (Data Warehouse Summary) layer: user-level feature aggregation
across full observation period and time windows (D1/D3/D7).

Bug-fix log:
  Bug-6: hard_map_battles not in Hive DDL -> dropped before Hive write
  Bug-7: insertInto positional mismatch -> explicit .select() for Hive
  Bug-8: total_battles ambiguity in join -> isolated join + drop

Semantic fixes applied:
  - fillna: count-type features fill 0, ratio-type features left as NULL
    (ratios are undefined when denominator is 0; filling 0 is misleading)
  - narrow_win_rate: clarified as fraction of ALL battles (not just wins)
"""

from pyspark.sql import functions as F
from pyspark.sql import Window

from config_env import env, env_bool

HIVE_DB    = env("TD_CHURN_HIVE_DB", "td_churn")
USE_HIVE   = env_bool("TD_CHURN_USE_HIVE", True)
OUTPUT_DIR = env("TD_CHURN_ETL_OUTPUT_DIR",
                 env("TD_CHURN_PROJECT_DIR", "/home/hadoop/td_churn_project")
                 + "/etl_output_v2")

TUTORIAL_MAP_ID = 1


# ──────────────── Sub-feature calculators ─────────────────────

def _calc_battle_stats(bl):
    """A. Full-period battle statistics."""
    return bl.groupBy("user_id").agg(
        F.count("*")                                        .alias("total_battles"),
        F.mean("battle_result")                             .alias("overall_win_rate"),
        F.mean("battle_duration_s")                         .alias("avg_battle_duration"),
        F.stddev("battle_duration_s")                       .alias("std_battle_duration"),
        F.mean("base_hp_ratio")                             .alias("avg_base_hp_ratio"),
        F.mean(F.when(F.col("battle_result") == 1,
                      F.col("base_hp_ratio")))              .alias("avg_win_base_hp"),
        F.sum("used_special_tower")                         .alias("total_special_tower_used"),
        F.mean("used_special_tower")                        .alias("special_tower_use_rate"),
        F.mean("tower_score")                               .alias("avg_tower_score"),
        F.max("tower_score")                                .alias("max_tower_score"),
        F.sum("wave_count")                                 .alias("total_waves_survived"),
    )


def _calc_progress(bl):
    """B. Level progression features."""
    return bl.groupBy("user_id").agg(
        F.max("map_id")                                     .alias("max_map_reached"),
        F.countDistinct("map_id")                           .alias("unique_maps_played"),
        F.mean("difficulty_num")                            .alias("avg_difficulty_played"),
        F.max("difficulty_num")                             .alias("max_difficulty_reached"),
        F.mean(F.when(F.col("difficulty_num") >= 3, 1)
                .otherwise(0))                              .alias("hard_map_ratio"),
    )


def _calc_stuck(bl):
    """C. Stuck/blocked features."""
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
    return feat_stuck.join(first_stuck, on="user_id", how="left")


def _calc_item_dependency(bl):
    """D. Item/tower dependency (Bug-6: hard_map_battles kept for Parquet only)."""
    feat_item_hard = bl.filter(F.col("difficulty_num") >= 3).groupBy("user_id").agg(
        F.mean("used_special_tower").alias("hard_map_special_rate"),
        F.count("*")               .alias("hard_map_battles"),
    )
    feat_item_fail = bl.filter(F.col("battle_result") == 0).groupBy("user_id").agg(
        F.mean("used_special_tower").alias("fail_special_rate"),
    )
    return feat_item_hard, feat_item_fail


def _calc_session_time(bl, session_df, feat_battle):
    """E. Session & time features (Bug-8: isolated join to avoid ambiguity)."""
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
    return feat_session, feat_time


def _calc_streak(bl):
    """F. Consecutive fail streak features."""
    return bl.groupBy("user_id").agg(
        F.max("consecutive_fail")                        .alias("max_consecutive_fail"),
        F.mean("consecutive_fail")                       .alias("avg_consecutive_fail"),
        F.mean(F.when(F.col("consecutive_fail") >= 3, 1)
                .otherwise(0))                           .alias("severe_fail_streak_ratio"),
    )


def _calc_recent(bl):
    """G. Recent trend (last 10 battles)."""
    return bl \
        .withColumn("rk", F.row_number().over(
            Window.partitionBy("user_id").orderBy(F.desc("battle_start_ts"))
        )) \
        .filter(F.col("rk") <= 10) \
        .groupBy("user_id").agg(
            F.mean("battle_result") .alias("recent10_win_rate"),
            F.mean("tower_score")   .alias("recent10_avg_score"),
            F.mean("difficulty_num").alias("recent10_avg_difficulty"),
        )


def _calc_map_meta_agg(bl):
    """H. Map metadata aggregation per user."""
    return bl.groupBy("user_id").agg(
        F.mean("map_clear_rate")      .alias("avg_map_clear_rate_played"),
        F.min("map_clear_rate")       .alias("min_map_clear_rate_played"),
        F.mean("map_avg_retry_times") .alias("avg_map_retry_rate_played"),
    )


def _calc_insights(bl):
    """
    I. Business insight features.

    narrow_win_rate: fraction of ALL battles that are narrow wins
        (is_narrow_win=1 means win with base_hp_ratio < 0.2).
        This is NOT narrow wins / total wins; it measures how often
        the player barely survives across all attempts.
    """
    feat_insights = bl.groupBy("user_id").agg(
        F.sum(F.col("is_narrow_win").cast("int"))    .alias("narrow_win_count"),
        F.mean(F.col("is_narrow_win").cast("double")).alias("narrow_win_rate"),
    )

    feat_first_attempt = (
        bl.filter(F.col("is_first_attempt") == 1)
        .groupBy("user_id")
        .agg(F.mean("battle_result").alias("first_attempt_win_rate"))
    )

    feat_retry = (
        bl.filter(F.col("is_first_attempt") == 0)
        .groupBy("user_id")
        .agg(F.countDistinct("map_id").alias("retry_map_count"))
    )
    return feat_insights, feat_first_attempt, feat_retry


# ──────────────── Time-window stats (D1/D3/D7) ───────────────

def _window_stats(bl, prefix, max_day):
    """Generic window aggregation for D3/D7."""
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


def _calc_d1(bl, session_df):
    """First-day features including session count (Bug-9 fix)."""
    feat_d1_base = bl.filter(F.col("day_index") == 1).groupBy("user_id").agg(
        F.count("*")                                 .alias("d1_battles"),
        F.mean("battle_result")                      .alias("d1_win_rate"),
        F.max("map_id")                              .alias("d1_max_map"),
        F.mean("used_special_tower")                 .alias("d1_special_rate"),
        F.sum(F.col("battle_duration_s") / 60)       .alias("d1_active_minutes"),
        F.max(F.when(F.col("map_id") == TUTORIAL_MAP_ID,
                     F.col("battle_result")))        .alias("d1_completed_map1"),
        F.countDistinct("map_id")                    .alias("d1_unique_maps"),
    )
    # Bug-9: use min(day_index) within session for D1 session count
    d1_sessions = session_df.filter(F.col("day_index") == 1) \
                            .groupBy("user_id") \
                            .agg(F.count("*").alias("d1_sessions"))
    return feat_d1_base \
        .join(d1_sessions, on="user_id", how="left") \
        .fillna({"d1_sessions": 0, "d1_completed_map1": 0})


# ──────────────── Main DWS builder ────────────────────────────

def build_dws(spark, bl, session_df):
    """
    Build all DWS feature tables and write to Hive / Parquet.

    Returns (dws_all, feat_d1, feat_d3, feat_d7).
    """
    print("\n[DWS] Feature aggregation ...")

    feat_battle = _calc_battle_stats(bl)
    feat_battle.cache()

    feat_progress                       = _calc_progress(bl)
    feat_stuck                          = _calc_stuck(bl)
    feat_item_hard, feat_item_fail      = _calc_item_dependency(bl)
    feat_session, feat_time             = _calc_session_time(bl, session_df, feat_battle)
    feat_streak                         = _calc_streak(bl)
    feat_recent                         = _calc_recent(bl)
    feat_meta                           = _calc_map_meta_agg(bl)
    feat_insights, feat_first_attempt, feat_retry = _calc_insights(bl)

    # Join all feature groups
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
        .join(feat_retry,         on="user_id", how="left")

    # FIX: separate fillna for count-type (fill 0) vs ratio-type (leave NULL).
    # Ratio features are undefined when denominator is 0; filling 0 is misleading.
    COUNT_FEATURES = [
        "stuck_level_count", "stuck_total_attempts",
        "hard_map_battles", "narrow_win_count", "retry_map_count",
    ]
    # Ratio features like narrow_win_rate, first_attempt_win_rate,
    # hard_map_special_rate, fail_special_rate, severe_fail_streak_ratio
    # are intentionally left as NULL when the user has no relevant battles.
    dws_all = dws_all.fillna(0, subset=COUNT_FEATURES)

    # Progress velocity: max_map / observation_span_days
    dws_all = dws_all.withColumn(
        "progress_velocity",
        F.col("max_map_reached").cast("double") /
        F.greatest(F.col("observation_span_days"), F.lit(1)).cast("double")
    )

    # Time-window features
    feat_d1 = _calc_d1(bl, session_df)
    feat_d3 = _window_stats(bl, "d3", 3)
    feat_d7 = _window_stats(bl, "d7", 7)

    print(f"  DWS full-period feature columns: {len(dws_all.columns)}")

    # Cache and materialise
    dws_all.cache()
    dws_all.count()

    # ── Hive write (Bug-6 & Bug-7: explicit select, drop extra columns) ──
    if USE_HIVE:
        dws_hive_cols = [
            "user_id",
            "total_battles", "overall_win_rate", "avg_battle_duration",
            "std_battle_duration", "avg_base_hp_ratio", "avg_win_base_hp",
            "total_special_tower_used", "special_tower_use_rate",
            "avg_tower_score", "max_tower_score", "total_waves_survived",
            "max_map_reached", "unique_maps_played", "avg_difficulty_played",
            "max_difficulty_reached", "hard_map_ratio",
            "stuck_level_count", "stuck_total_attempts", "first_stuck_map_id",
            "hard_map_special_rate",
            "fail_special_rate",
            "active_days", "total_sessions", "avg_battles_per_session",
            "avg_session_duration_s", "avg_session_gap_s", "max_session_gap_s",
            "battles_per_active_day", "observation_span_days",
            "max_consecutive_fail", "avg_consecutive_fail", "severe_fail_streak_ratio",
            "recent10_win_rate", "recent10_avg_score", "recent10_avg_difficulty",
            "avg_map_clear_rate_played", "min_map_clear_rate_played",
            "avg_map_retry_rate_played",
            "narrow_win_count", "narrow_win_rate", "first_attempt_win_rate",
            "retry_map_count", "progress_velocity",
        ]
        spark.sql(f"DROP TABLE IF EXISTS {HIVE_DB}.dws_user_battle_stats")
        dws_all.select(dws_hive_cols) \
               .write.mode("overwrite").format("orc") \
               .saveAsTable(f"{HIVE_DB}.dws_user_battle_stats")

        spark.sql(f"DROP TABLE IF EXISTS {HIVE_DB}.dws_user_d1_stats")
        feat_d1.select(["user_id", "d1_battles", "d1_win_rate", "d1_max_map",
                        "d1_completed_map1", "d1_special_rate",
                        "d1_active_minutes", "d1_sessions", "d1_unique_maps"]) \
               .write.mode("overwrite").format("orc") \
               .saveAsTable(f"{HIVE_DB}.dws_user_d1_stats")

        spark.sql(f"DROP TABLE IF EXISTS {HIVE_DB}.dws_user_d3_stats")
        feat_d3.select(["user_id", "d3_battles", "d3_win_rate", "d3_max_map",
                        "d3_active_days", "d3_avg_daily_battles",
                        "d3_stuck_count", "d3_special_rate"]) \
               .write.mode("overwrite").format("orc") \
               .saveAsTable(f"{HIVE_DB}.dws_user_d3_stats")

        spark.sql(f"DROP TABLE IF EXISTS {HIVE_DB}.dws_user_d7_stats")
        feat_d7.select(["user_id", "d7_battles", "d7_win_rate", "d7_max_map",
                        "d7_active_days", "d7_avg_daily_battles",
                        "d7_stuck_count", "d7_special_rate"]) \
               .write.mode("overwrite").format("orc") \
               .saveAsTable(f"{HIVE_DB}.dws_user_d7_stats")
    else:
        dws_all.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/dws_user_battle_stats")
        feat_d1.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/dws_user_d1_stats")
        feat_d3.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/dws_user_d3_stats")
        feat_d7.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/dws_user_d7_stats")

    return dws_all, feat_d1, feat_d3, feat_d7
