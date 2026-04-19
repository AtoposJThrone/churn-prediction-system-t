"""
ods.schema
==========
Hive DDL definitions for ODS-layer tables.
Ensures all required tables exist before data loading.
"""

from config_env import env


HIVE_DB        = env("TD_CHURN_HIVE_DB", "td_churn")
WAREHOUSE_DIR  = env("TD_CHURN_HIVE_WAREHOUSE_DIR", "/user/hive/warehouse")

ODS_BATTLE_LOG_LOCATION = f"{WAREHOUSE_DIR}/{HIVE_DB}.db/ods_battle_log"
ODS_MAP_META_LOCATION   = f"{WAREHOUSE_DIR}/{HIVE_DB}.db/ods_map_meta"
ODS_USER_LABEL_LOCATION = f"{WAREHOUSE_DIR}/{HIVE_DB}.db/ods_user_label"


def ensure_hive_tables(spark):
    """
    Create ODS external tables if they do not already exist.

    - ods_battle_log: partitioned by dt (battle date string)
    - ods_map_meta: non-partitioned
    - ods_user_label: non-partitioned
    """
    print("\n[HIVE] Checking / creating ODS tables ...")

    # ── ods_battle_log: long-lived partitioned table ──
    spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {HIVE_DB}.ods_battle_log (
        user_id            INT,
        map_id             INT,
        battle_result      INT,
        battle_duration_s  FLOAT,
        base_hp_ratio      FLOAT,
        used_special_tower INT,
        battle_start_time  STRING,
        session_gap_s      FLOAT,
        cumulative_battles INT,
        consecutive_fail   INT,
        difficulty_tier    STRING,
        wave_count         INT,
        tower_score        FLOAT,
        is_narrow_win      INT,
        is_first_attempt   INT
    )
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '{ODS_BATTLE_LOG_LOCATION}'
    """)

    # ── ods_map_meta: external non-partitioned table ──
    spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {HIVE_DB}.ods_map_meta (
        map_id                 INT,
        map_avg_duration_s     FLOAT,
        map_clear_rate         FLOAT,
        map_avg_win_duration_s FLOAT,
        map_avg_retry_times    FLOAT,
        difficulty_tier        STRING,
        map_design_waves       INT
    )
    STORED AS ORC
    LOCATION '{ODS_MAP_META_LOCATION}'
    """)

    # ── ods_user_label: external non-partitioned table ──
    spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {HIVE_DB}.ods_user_label (
        user_id INT,
        label   INT,
        split   STRING
    )
    STORED AS ORC
    LOCATION '{ODS_USER_LABEL_LOCATION}'
    """)

    print("  Done: ODS tables verified")
