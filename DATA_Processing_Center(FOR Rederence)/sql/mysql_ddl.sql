-- =============================================================
-- mysql_ddl.sql
-- 塔防游戏流失预警系统 —— MySQL 版本建库建表语句
-- 原为 Hive 数仓分层脚本，现转为 MySQL 语法，保留所有注释
-- 注意：分区字段 dt 已转为普通列（原 Hive 分区逻辑需由应用层处理）
-- =============================================================

-- ─────────────────────────────────────────────────────────────
-- 数据库
-- ─────────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS td_churn;
USE td_churn;


-- =============================================================
-- ODS 层：原始数据层（贴源存储，不做任何加工）
-- =============================================================

-- ODS: 战斗日志原始表（对应 battle_log.csv，按日分区）
CREATE TABLE IF NOT EXISTS ods_battle_log (
    user_id             INT         COMMENT '用户ID',
    map_id              INT         COMMENT '地图ID',
    battle_result       TINYINT     COMMENT '战斗结果：1=胜，0=败',
    battle_duration_s   FLOAT       COMMENT '战斗时长（秒）',
    base_hp_ratio       FLOAT       COMMENT '基地剩余血量比例',
    used_special_tower  TINYINT     COMMENT '是否使用特殊塔/技能',
    battle_start_time   VARCHAR(255) COMMENT '战斗开始时间（字符串原样存储）',
    session_gap_s       FLOAT       COMMENT '与上一场间隔（秒）',
    cumulative_battles  INT         COMMENT '累计战斗场次',
    consecutive_fail    INT         COMMENT '当前连续失败次数',
    difficulty_tier     VARCHAR(50) COMMENT '地图难度分级',
    wave_count          INT         COMMENT '经历波次数',
    tower_score         FLOAT       COMMENT '防守综合评分',
    dt                  VARCHAR(10) COMMENT '数据日期 yyyy-MM-dd（原Hive分区字段）'
) COMMENT = 'ODS层-战斗日志原始表';

-- ODS: 地图元数据表（全量，无分区）
CREATE TABLE IF NOT EXISTS ods_map_meta (
    map_id                  INT         COMMENT '地图ID',
    map_avg_duration_s      FLOAT       COMMENT '地图平均战斗时长（秒）',
    map_clear_rate          FLOAT       COMMENT '地图平均通关率',
    map_avg_win_duration_s  FLOAT       COMMENT '通关场次平均耗时（秒）',
    map_avg_retry_times     FLOAT       COMMENT '平均重试次数',
    difficulty_tier         VARCHAR(50) COMMENT '难度分级',
    map_design_waves        INT         COMMENT '设计波次数'
) COMMENT = 'ODS层-地图元数据表';

-- ODS: 用户标签表（train/dev/test 合并后导入）
CREATE TABLE IF NOT EXISTS ods_user_label (
    user_id   INT         COMMENT '用户ID',
    label     TINYINT     COMMENT '流失标签：1=流失，0=留存，NULL=测试集',
    split     VARCHAR(10) COMMENT '数据集划分：train/dev/test'
) COMMENT = 'ODS层-用户流失标签表';


-- =============================================================
-- DWD 层：明细数据层（清洗、标准化、打宽，保留行级明细）
-- =============================================================

-- DWD: 战斗明细表（清洗后，补充注册日、游戏天数索引、会话ID）
CREATE TABLE IF NOT EXISTS dwd_battle_detail (
    user_id               INT         COMMENT '用户ID',
    map_id                INT         COMMENT '地图ID',
    battle_result         TINYINT     COMMENT '战斗结果',
    battle_duration_s     FLOAT       COMMENT '战斗时长（秒）',
    base_hp_ratio         FLOAT       COMMENT '基地剩余血量比例',
    used_special_tower    TINYINT     COMMENT '是否使用特殊塔',
    battle_start_ts       BIGINT      COMMENT '战斗开始时间戳（秒级Unix时间戳）',
    battle_date           VARCHAR(10) COMMENT '战斗日期 yyyy-MM-dd',
    session_gap_s         FLOAT       COMMENT '与上一场间隔（秒）',
    session_id            VARCHAR(100) COMMENT '会话ID（user_id + 会话序号）',
    cumulative_battles    INT         COMMENT '累计战斗场次',
    consecutive_fail      INT         COMMENT '当前连续失败次数',
    is_stuck              TINYINT     COMMENT '是否为卡关状态（同一地图连续失败≥2次）',
    day_index             INT         COMMENT '注册后第几天（1-based）',
    is_day1               TINYINT     COMMENT '是否为注册首日行为',
    wave_count            INT         COMMENT '经历波次数',
    tower_score           FLOAT       COMMENT '防守综合评分',
    difficulty_num        INT         COMMENT '难度数值（1-4）',
    map_clear_rate        FLOAT       COMMENT '地图通关率',
    map_avg_retry_times   FLOAT       COMMENT '地图平均重试次数',
    dt                    VARCHAR(10) COMMENT '数据日期（原Hive分区字段）'
) COMMENT = 'DWD层-战斗行为明细表';

-- DWD: 会话明细表（以会话为粒度）
CREATE TABLE IF NOT EXISTS dwd_session_detail (
    user_id              INT         COMMENT '用户ID',
    session_id           VARCHAR(100) COMMENT '会话ID',
    session_date         VARCHAR(10) COMMENT '会话日期',
    session_start_ts     BIGINT      COMMENT '会话开始时间戳',
    session_end_ts       BIGINT      COMMENT '会话结束时间戳',
    session_duration_s   BIGINT      COMMENT '会话时长（秒）',
    session_battles      INT         COMMENT '会话内战斗场次',
    session_win_count    INT         COMMENT '会话内胜利场次',
    day_index            INT         COMMENT '注册后第几天',
    dt                   VARCHAR(10) COMMENT '数据日期（原Hive分区字段）'
) COMMENT = 'DWD层-会话明细表';


-- =============================================================
-- DWS 层：汇总数据层（按用户聚合，输出多个时间窗口的特征）
-- =============================================================

-- DWS: 用户全周期战斗统计（全量）
CREATE TABLE IF NOT EXISTS dws_user_battle_stats (
    user_id                     INT    COMMENT '用户ID',
    -- 战斗统计
    total_battles               INT    COMMENT '总战斗场次',
    overall_win_rate            FLOAT  COMMENT '整体胜率',
    avg_battle_duration         FLOAT  COMMENT '平均战斗时长',
    std_battle_duration         FLOAT  COMMENT '战斗时长标准差',
    avg_base_hp_ratio           FLOAT  COMMENT '平均基地剩余血量',
    avg_win_base_hp             FLOAT  COMMENT '胜利场平均基地血量',
    total_special_tower_used    INT    COMMENT '特殊塔累计使用次数',
    special_tower_use_rate      FLOAT  COMMENT '特殊塔使用率',
    avg_tower_score             FLOAT  COMMENT '平均防守评分',
    max_tower_score             FLOAT  COMMENT '最高防守评分',
    total_waves_survived        INT    COMMENT '累计经历波次',
    -- 关卡进度
    max_map_reached             INT    COMMENT '最高到达地图',
    unique_maps_played          INT    COMMENT '游玩地图种类数',
    avg_difficulty_played       FLOAT  COMMENT '平均游玩难度',
    max_difficulty_reached      INT    COMMENT '最高难度',
    hard_map_ratio              FLOAT  COMMENT '高难度地图占比',
    -- 卡关特征
    stuck_level_count           INT    COMMENT '发生卡关的地图数量',
    stuck_total_attempts        INT    COMMENT '卡关地图总尝试次数',
    first_stuck_map_id          INT    COMMENT '首次卡关地图ID',
    -- 道具依赖
    hard_map_special_rate       FLOAT  COMMENT '高难度地图特殊塔使用率',
    fail_special_rate           FLOAT  COMMENT '失败场特殊塔使用率',
    -- 时序行为
    active_days                 INT    COMMENT '活跃天数',
    total_sessions              INT    COMMENT '总会话数',
    avg_battles_per_session     FLOAT  COMMENT '每会话平均战斗场次',
    avg_session_duration_s      FLOAT  COMMENT '平均会话时长（秒）',
    avg_session_gap_s           FLOAT  COMMENT '平均场次间隔（秒）',
    max_session_gap_s           FLOAT  COMMENT '最大场次间隔（秒）',
    battles_per_active_day      FLOAT  COMMENT '日均战斗场次',
    observation_span_days       INT    COMMENT '观测窗口跨度（天）',
    -- 连败压力
    max_consecutive_fail        INT    COMMENT '最大连续失败次数',
    avg_consecutive_fail        FLOAT  COMMENT '平均连续失败次数',
    severe_fail_streak_ratio    FLOAT  COMMENT '连败≥3次的场次占比',
    -- 近期趋势（最近10场）
    recent10_win_rate           FLOAT  COMMENT '近10场胜率',
    recent10_avg_score          FLOAT  COMMENT '近10场平均评分',
    recent10_avg_difficulty     FLOAT  COMMENT '近10场平均难度',
    -- 地图元数据聚合
    avg_map_clear_rate_played   FLOAT  COMMENT '游玩地图平均通关率',
    min_map_clear_rate_played   FLOAT  COMMENT '游玩地图最低通关率',
    avg_map_retry_rate_played   FLOAT  COMMENT '游玩地图平均重试率'
) COMMENT = 'DWS层-用户全周期战斗统计';

-- DWS: 用户 D1（注册首日）行为特征
CREATE TABLE IF NOT EXISTS dws_user_d1_stats (
    user_id               INT    COMMENT '用户ID',
    d1_battles            INT    COMMENT '首日战斗场次',
    d1_win_rate           FLOAT  COMMENT '首日胜率',
    d1_max_map            INT    COMMENT '首日达到最高地图',
    d1_completed_map1     TINYINT COMMENT '首日是否完成第1关（新手引导）',
    d1_special_rate       FLOAT  COMMENT '首日特殊塔使用率',
    d1_active_minutes     FLOAT  COMMENT '首日活跃时长（分钟）',
    d1_sessions           INT    COMMENT '首日会话数'
) COMMENT = 'DWS层-用户首日(D1)行为特征';

-- DWS: 用户 D3（前3天）行为特征
CREATE TABLE IF NOT EXISTS dws_user_d3_stats (
    user_id               INT    COMMENT '用户ID',
    d3_battles            INT    COMMENT '前3天战斗场次',
    d3_win_rate           FLOAT  COMMENT '前3天胜率',
    d3_max_map            INT    COMMENT '前3天最高地图',
    d3_active_days        INT    COMMENT '前3天活跃天数',
    d3_avg_daily_battles  FLOAT  COMMENT '前3天日均战斗',
    d3_stuck_count        INT    COMMENT '前3天卡关次数',
    d3_special_rate       FLOAT  COMMENT '前3天特殊塔使用率'
) COMMENT = 'DWS层-用户前3天(D3)行为特征';

-- DWS: 用户 D7（前7天）行为特征（结构同 D3，字段名 d3 → d7）
CREATE TABLE IF NOT EXISTS dws_user_d7_stats (
    user_id               INT    COMMENT '用户ID',
    d7_battles            INT    COMMENT '前7天战斗场次',
    d7_win_rate           FLOAT  COMMENT '前7天胜率',
    d7_max_map            INT    COMMENT '前7天最高地图',
    d7_active_days        INT    COMMENT '前7天活跃天数',
    d7_avg_daily_battles  FLOAT  COMMENT '前7天日均战斗',
    d7_stuck_count        INT    COMMENT '前7天卡关次数',
    d7_special_rate       FLOAT  COMMENT '前7天特殊塔使用率'
) COMMENT = 'DWS层-用户前7天(D7)行为特征';


-- =============================================================
-- ADS 层：应用数据层（直接对接 SpringBoot / 可视化）
-- =============================================================

-- ADS: 用户流失风险评分表（每日跑批更新）
CREATE TABLE IF NOT EXISTS ads_user_churn_risk (
    user_id             INT         COMMENT '用户ID',
    churn_prob          FLOAT       COMMENT '模型预测流失概率（0-1）',
    risk_level          VARCHAR(10) COMMENT '风险等级：high/medium/low',
    rule_trigger        TEXT        COMMENT '命中的规则列表（JSON字符串）',
    final_alert         TINYINT     COMMENT '最终预警结果：1=预警，0=安全',
    top_reason_1        VARCHAR(255) COMMENT '主要流失原因1',
    top_reason_2        VARCHAR(255) COMMENT '主要流失原因2',
    top_reason_3        VARCHAR(255) COMMENT '主要流失原因3',
    predict_dt          VARCHAR(10) COMMENT '预测日期',
    model_version       VARCHAR(50) COMMENT '使用的模型版本',
    dt                  VARCHAR(10) COMMENT '跑批日期（原Hive分区字段）'
) COMMENT = 'ADS层-用户流失风险预警结果';

-- ADS: 每日流失态势汇总（面向大屏展示）
CREATE TABLE IF NOT EXISTS ads_daily_churn_summary (
    stat_date            VARCHAR(10) COMMENT '统计日期',
    total_active_users   INT         COMMENT '当日活跃用户数',
    high_risk_count      INT         COMMENT '高风险用户数',
    medium_risk_count    INT         COMMENT '中风险用户数',
    low_risk_count       INT         COMMENT '低风险用户数',
    high_risk_rate       FLOAT       COMMENT '高风险占比',
    avg_churn_prob       FLOAT       COMMENT '全体平均流失概率',
    top_stuck_map_id     INT         COMMENT '当日卡关最多的地图ID',
    d1_no_tutorial_count INT         COMMENT '首日未完成新手引导用户数'
) COMMENT = 'ADS层-每日流失态势汇总';


-- =============================================================
-- 原文件中的数据加载示例（Hive 特有，已移除）
-- MSCK REPAIR TABLE 等命令不适用于 MySQL
-- =============================================================
