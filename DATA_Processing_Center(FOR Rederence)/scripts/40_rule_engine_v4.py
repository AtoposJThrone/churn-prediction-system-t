"""
rule_engine.py
==============
规则-模型混合预警策略

开题报告第 2.3(4) 节：
  "先用规则引擎识别明显异常行为（如连续7日未登录、首日未完成首关新手引导等），
   再通过机器学习模型评估流失概率"

预警流程
--------
                    ┌──────────────────────────────────┐
  用户特征 ─────→   │  1. 规则引擎（硬规则）             │
                    │     命中 → 直接标记高风险           │
                    │     未命中 → 进入模型评估           │
                    └──────────────┬───────────────────┘
                                   ↓
                    ┌──────────────────────────────────┐
                    │  2. 模型评分（流失概率 0-1）        │
                    │     ≥ 0.7  → 高风险               │
                    │  0.4-0.7   → 中风险               │
                    │     < 0.4  → 低风险               │
                    └──────────────┬───────────────────┘
                                   ↓
                    ┌──────────────────────────────────┐
                    │  3. 流失原因分析（SHAP / 规则描述）│
                    │     输出 top3 流失原因             │
                    └──────────────┬───────────────────┘
                                   ↓
                              预警结果写出
"""

import os
import json
import pandas as pd
import numpy as np

from config_env import env, env_int

# ───────────────────────────────────────────────────────────────
# [MODIFIED 2026-03-04] 实操发现 Spark RF 最优：从 LightGBM 推理切换到 Spark MLlib RF 推理
#   - 原 lightgbm import 保留但注释掉（方便回滚/对比）
#   - 新增 PipelineModel / RandomForestClassificationModel 加载
# ───────────────────────────────────────────────────────────────
# import lightgbm as lgb   # [OLD-LGBM] 原方案：本地 LightGBM Booster 推理

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType, IntegerType

# [MODIFIED] Spark MLlib 推理所需
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler, Imputer
from pyspark.ml.classification import RandomForestClassificationModel

# vector_to_array 在 Spark 3.x 通常可用；为了兼容更老版本，提供 UDF 兜底
try:
    from pyspark.ml.functions import vector_to_array
except Exception:  # pragma: no cover
    vector_to_array = None
    from pyspark.ml.linalg import VectorUDT

    @F.udf("array<double>")
    def vector_to_array(v):
        if v is None:
            return [None, None]
        try:
            return v.toArray().tolist()
        except Exception:
            return [None, None]


# ──────────────── 配置 ────────────────
# MODEL_PATH   = "${TD_CHURN_LGBM_MODEL_PATH}"    # [OLD-LGBM] 原本可切换为 LightGBM 单文件模型
# [MODIFIED] Spark MLlib 模型是“目录”，不是单文件；YARN/HDFS 上可直接 load
PROJECT_DIR = env("TD_CHURN_PROJECT_DIR", "/home/hadoop/td_churn_project")
MODEL_DIR = env("TD_CHURN_MODEL_DIR", f"{PROJECT_DIR}/models")
MODEL_PATH = env("TD_CHURN_RF_MODEL_PATH", f"{MODEL_DIR}/rf_model")
HIVE_DB = env("TD_CHURN_HIVE_DB", "td_churn")  # Hive 库名，用于读取 DWD 表聚合关卡热力图

FEATURE_DIR = env("TD_CHURN_ETL_OUTPUT_DIR", f"{PROJECT_DIR}/etl_output_v2")
OUTPUT_DIR = env("TD_CHURN_ALERT_OUTPUT_DIR", f"{PROJECT_DIR}/alert_output")
MYSQL_URL = env("TD_CHURN_DB_URL", required=True)
MYSQL_USER = env("TD_CHURN_DB_USER", required=True)
MYSQL_PASS = env("TD_CHURN_DB_PASSWORD", required=True)

# [MODIFIED] 必须使用“训练时同一套”特征 Pipeline（Imputer→Assembler→Scaler）
#   - 需要在 32_train.py 里把 pipeline_model 也保存下来（建议路径如下）
#   - 如果该路径 load 失败，本脚本会 fallback：在当前数据上 fit 一套 pipeline（会给出 WARN）
PIPELINE_PATH = env("TD_CHURN_PIPELINE_MODEL_PATH", f"{MODEL_DIR}/spark_feature_pipeline")

HIGH_RISK_THRESHOLD = 0.66   # 模型分数 ≥ 此值 → 高风险
MEDIUM_RISK_THRESHOLD = 0.47  # 模型分数 ≥ 此值 → 中风险
# 以上模型分数来自dev_sdf的计算结果
SHUFFLE_PARTITIONS = str(env_int("TD_CHURN_SPARK_SHUFFLE_PARTITIONS", 100))

os.makedirs(OUTPUT_DIR, exist_ok=True)

# [MODIFIED] 推理侧推断特征列时需要排除的非特征列（与 32_train.py 保持一致）
NON_FEAT = {"user_id", "label", "split", "first_stuck_map_id"}


# ══════════════════════════════════════════════════════════════
# 1. 规则定义
# ══════════════════════════════════════════════════════════════
"""
每条规则是一个 dict：
    code        : 规则唯一代码
    name        : 人类可读名称
    description : 向运营人员展示的原因说明
    condition   : 接收 user_row(dict) 返回 bool 的函数
    force_high  : True → 命中即直接标记高风险，跳过模型
"""

RULES = [
    {
        "code": "R001",
        "name": "首日未完成新手引导",
        "description": "用户注册首日未通过第1关（新手引导），对游戏核心循环理解不足，流失风险极高",
        "condition": lambda r: r.get("d1_completed_map1", 1) == 0
        and r.get("d1_battles", 0) >= 1,
        "force_high": True,
    },
    {
        "code": "R002",
        "name": "首日零战斗",
        "description": "用户注册首日未进行任何战斗，可能注册后立即流失",
        "condition": lambda r: r.get("d1_battles", 0) == 0,
        "force_high": True,
    },
    {
        "code": "R003",
        "name": "严重卡关（同一地图卡死）",
        "description": f"用户存在明显卡关行为（高难度地图连续多次失败且胜率极低），"
        f"挫败感导致流失风险上升",
        "condition": lambda r: r.get("stuck_level_count", 0) >= 3
        and r.get("overall_win_rate", 1) < 0.3,
        "force_high": False,
    },
    {
        "code": "R004",
        "name": "近期胜率骤降",
        "description": "用户最近10场胜率显著低于整体胜率（下降超过30%），游戏体验恶化",
        "condition": lambda r: (r.get("overall_win_rate", 0) - r.get("recent10_win_rate", 0)) > 0.30
        and r.get("total_battles", 0) >= 20,
        "force_high": False,
    },
    {
        "code": "R005",
        "name": "会话间隔异常拉长",
        "description": "用户最近平均会话间隔超过12小时，活跃度已显著下降",
        "condition": lambda r: r.get("avg_session_gap_s", 0) > 43200,
        "force_high": False,
    },
    {
        "code": "R006",
        "name": "高道具依赖但仍频繁失败",
        "description": "用户高频使用特殊塔但胜率仍低，说明技能与关卡难度严重不匹配",
        "condition": lambda r: r.get("special_tower_use_rate", 0) > 0.7
        and r.get("overall_win_rate", 1) < 0.4,
        "force_high": False,
    },
    {
        "code": "R007",
        "name": "前3天活跃天数不足",
        "description": "注册后前3天仅活跃1天，早期留存信号极差",
        "condition": lambda r: r.get("d3_active_days", 3) <= 1 and r.get("total_battles", 0) > 0,
        "force_high": False,
    },
    {
        "code": "R008",
        "name": "关卡进度停滞",
        "description": "用户已有一定活跃天数但关卡推进极慢（每天不足半关），说明玩家正反馈不足，随时可能润",
        "condition": lambda r: r.get("progress_velocity", 1.0) < 0.5 and r.get("active_days", 0) >= 3,
        "force_high": False,
    },
    {
        "code": "R009",
        "name": "首日卡在第一关",
        "description": "用户首日只接触了一个关卡却反复尝试，说明新手引导设计存在缺陷或玩家能力较差",
        "condition": lambda r: r.get("d1_unique_maps", 2) == 1 and r.get("d1_battles", 0) >= 3,
        "force_high": False,
    },
]


def apply_rules(user_row: dict) -> tuple[bool, list[str], list[str]]:
    """
    对单条用户数据应用规则引擎。

    Returns
    -------
    force_high  : 是否强制标记为高风险
    triggered   : 命中的规则 code 列表
    descriptions: 命中规则的描述列表
    """
    force_high = False
    triggered = []
    descriptions = []

    for rule in RULES:
        try:
            if rule["condition"](user_row):
                triggered.append(rule["code"])
                descriptions.append(rule["description"])
                if rule["force_high"]:
                    force_high = True
        except Exception:
            pass  # 容错：特征缺失时跳过该规则

    return force_high, triggered, descriptions


# ══════════════════════════════════════════════════════════════
# 2. 流失原因分析（特征贡献度 → 可读文本）
# ══════════════════════════════════════════════════════════════
# 特征 → 运营可读原因的映射模板
FEATURE_REASON_MAP = {
    "d1_completed_map1": "首日未通过新手引导关卡",
    "d1_battles": "首日战斗场次极少",
    "overall_win_rate": "整体胜率偏低",
    "recent10_win_rate": "近期胜率下滑明显",
    "max_consecutive_fail": "曾出现长时间连败",
    "stuck_level_count": "在多个关卡反复卡关",
    "avg_session_gap_s": "游戏会话间隔过长",
    "active_days": "整体活跃天数少",
    "d3_active_days": "前3天活跃度低",
    "hard_map_special_rate": "高难度关卡过度依赖道具",
    "fail_special_rate": "失败时仍大量消耗道具（体验差）",
    "battles_per_active_day": "日均战斗场次少，粘性不足",
    "max_map_reached": "关卡进度停滞",
    "avg_tower_score": "防守表现持续较差",
    # 新增业务洞察特征对应的流失原因
    "narrow_win_rate": "残血局多，长期处于高压力局面（小失即导致流失）",
    "first_attempt_win_rate": "首次尝试胜率低，关卡难度进阶时沉没感强",
    "progress_velocity": "关卡推进速度极慢，玩家处于游戏瓶颈期",
    "d1_unique_maps": "首日仅探索了一个关卡且多次尝试失败，入门关设计缺陷明显",
}

# def get_top_reasons(user_row: dict, model: lgb.Booster, feature_names: list,
#                     rule_descs: list, n=3) -> list[str]:
#     """  # [OLD-LGBM] 原函数签名带 lgb.Booster（但函数体并未真正使用 model）
#     综合规则触发描述和特征贡献分析，输出 Top-N 流失原因。
#     """
#     ...

# [MODIFIED] 去掉对 lightgbm 的类型依赖，避免 RF 推理时因为未安装 lightgbm 导致 NameError
def get_top_reasons(user_row: dict, rule_descs: list, n=3) -> list[str]:
    """
    综合规则触发描述和特征启发式分析，输出 Top-N 流失原因。

    说明：
    - 当前实现并未真正计算 SHAP（原注释保留），而是：
      1) 优先输出规则触发的原因描述
      2) 不足 n 条时，用关键特征的“坏信号阈值”补足
    """
    reasons = list(rule_descs)  # 规则触发的原因优先

    if len(reasons) < n:
        # 用特征值异常程度补充
        # （简单启发式：偏离均值最多的关键特征）
        for feat, reason_text in FEATURE_REASON_MAP.items():
            if feat in user_row and reason_text not in reasons:
                val = user_row.get(feat, 0) or 0
                # 方向判断：低值为坏信号的特征
                bad_low = {
                    "overall_win_rate",
                    "recent10_win_rate",
                    "active_days",
                    "d1_battles",
                    "d3_active_days",
                    "battles_per_active_day",
                    "max_map_reached",
                    "avg_tower_score",
                    "d1_completed_map1",
                    "first_attempt_win_rate",  # 新增：首次尝试胜率低是坏信号
                    "progress_velocity",       # 新增：进度速率极慢是坏信号
                    "d1_unique_maps",           # 新增：首日只接触 1 个关卡是坏信号
                }
                bad_high = {
                    "avg_session_gap_s",
                    "max_consecutive_fail",
                    "stuck_level_count",
                    "fail_special_rate",
                    "narrow_win_rate",  # 新增：险胜率持续过高说明长期处于高压力局面
                }

                if (feat in bad_low and val < 0.3) or (feat in bad_high and val > 0.7):
                    reasons.append(reason_text)

            if len(reasons) >= n:
                break

    return reasons[:n]


# ══════════════════════════════════════════════════════════════
# [MODIFIED] 2.5 推理侧 Pipeline 兜底构建（当 PIPELINE_PATH load 失败时）
# ══════════════════════════════════════════════════════════════
def build_pipeline_fallback(sdf):
    """
    在当前数据上临时 fit 一套 Imputer→Assembler→Scaler Pipeline（兜底方案）。
    ⚠️ 注意：理论上应该 load 训练时保存的 pipeline；此函数仅用于 PIPELINE_PATH 不存在时的应急。
    """
    numeric_types = {"double", "float", "integer", "long"}
    feat_cols = [
        c
        for c in sdf.columns
        if c not in NON_FEAT and sdf.schema[c].dataType.typeName() in numeric_types
    ]

    imp_out = [f"{c}_imp" for c in feat_cols]
    imputer = Imputer(inputCols=feat_cols, outputCols=imp_out, strategy="mean")
    assembler = VectorAssembler(inputCols=imp_out, outputCol="features_raw", handleInvalid="keep")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
    pipeline = Pipeline(stages=[imputer, assembler, scaler])

    pipeline_model = pipeline.fit(sdf)
    return pipeline_model, feat_cols


# ══════════════════════════════════════════════════════════════
# [MODIFIED_v3] MySQL 辅助：执行清空/删除等 SQL（避免 overwrite 触发 DROP）
# ══════════════════════════════════════════════════════════════
def mysql_exec_sql(spark, url: str, user: str, password: str, sql: str):
    """
    用 Spark 的 JVM 直接通过 JDBC 执行一条 MySQL SQL（DELETE/TRUNCATE等）。
    这样可以在写入前做“清空/按日期删除”，不需要 DROP 权限。
    """
    jvm = spark._sc._gateway.jvm
    # 确保 Driver 注册到 DriverManager
    jvm.java.lang.Class.forName("com.mysql.cj.jdbc.Driver")
    conn = jvm.java.sql.DriverManager.getConnection(url, user, password)
    try:
        stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════
# 3. 主预警流程
# ══════════════════════════════════════════════════════════════
def run_alert_pipeline():
    # ───────────────────────────────────────────────────────────
    # [MODIFIED_v2] 统一跑批日期（用于：Hive 分区 dt、MySQL 覆盖、predict_dt/stat_date、输出文件）
    # ───────────────────────────────────────────────────────────
    RUN_DT = pd.Timestamp.today().strftime("%Y-%m-%d")

    # spark = SparkSession.builder \
    #     .appName("TowerDefense_RuleEngine") \
    #     .config("spark.sql.shuffle.partitions", "100") \
    #     .getOrCreate()
    #
    # [MODIFIED_v2] enableHiveSupport()：写 Hive td_churn.ads_* 表必备
    spark = (
        SparkSession.builder
        .appName("TowerDefense_RuleEngine")
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .enableHiveSupport()  # [MODIFIED_v2]
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # [MODIFIED_v2] 一些写 Hive 分区/覆盖时常用的配置（不影响 MySQL）
    try:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")  # [MODIFIED_v2]
    except Exception:
        pass

    # 3a. 加载特征（全量，含 test）
    print("[ALERT] 加载特征表 ...")
    test_sdf = spark.read.parquet(f"{FEATURE_DIR}/features_test.parquet")
    # 也可对 train/dev 做回测验证
    all_sdf = test_sdf

    # ───────────────────────────────────────────────────────────
    # 3b. 转 pandas（规则引擎和模型推理均在 driver 侧）
    # feature_df = all_sdf.toPandas()
    # print(f"  待预警用户数: {len(feature_df):,}")
    #
    # 3c. 加载模型
    # model = lgb.Booster(model_file=MODEL_PATH)
    # feature_names = model.feature_name()
    #
    # 3d. 模型打分
    # X = feature_df[feature_names].fillna(feature_df[feature_names].median())
    # feature_df["churn_prob"] = model.predict(X)
    # ───────────────────────────────────────────────────────────
    # [OLD-LGBM] ↑↑↑ 原方案：toPandas + LightGBM 预测（保留，便于回滚/对比）
    # ───────────────────────────────────────────────────────────

    # ───────────────────────────────────────────────────────────
    # [MODIFIED] 3b-3d：先在 Spark 侧完成 Pipeline + RF 打分，再 toPandas 做规则
    #   目的：1) 模型与 YARN/HDFS 输出一致；2) 减少 toPandas 数据量（只取必要列）
    # ───────────────────────────────────────────────────────────
    print("[ALERT] 加载 Spark Pipeline & RF 模型 ...")
    feat_cols = None
    try:
        pipeline_model = PipelineModel.load(PIPELINE_PATH)
        # 尝试从 pipeline 中取出 assembler 的 inputCols 作为参考（可选）
        try:
            for stg in pipeline_model.stages:
                if hasattr(stg, "getInputCols") and hasattr(stg, "getOutputCol"):
                    if stg.getOutputCol() == "features_raw":
                        feat_cols = stg.getInputCols()
                        break
        except Exception:
            pass
        print(f"  Pipeline load 成功: {PIPELINE_PATH}")
    except Exception as e:
        print(f"  [WARN] Pipeline load 失败，将在当前数据上临时 fit 一套 Pipeline: {e}")
        pipeline_model, feat_cols = build_pipeline_fallback(all_sdf)

    rf_model = RandomForestClassificationModel.load(MODEL_PATH)
    print(f"  RF 模型 load 成功: {MODEL_PATH}")

    # Spark 侧打分
    proc_sdf = pipeline_model.transform(all_sdf)
    pred_sdf = rf_model.transform(proc_sdf)

    # 取正类概率作为 churn_prob
    # probability 向量一般为 [P(label=0), P(label=1)]
    pred_sdf = pred_sdf.withColumn(
        "churn_prob", vector_to_array(F.col("probability"))[1].cast("double")
    )

    # 为减少 driver 内存占用：只 select 规则/原因所需字段 + churn_prob
    required_cols = set(["user_id"]) | set(FEATURE_REASON_MAP.keys()) | set(
        [
            # RULES 中直接用到的列（即便不在 FEATURE_REASON_MAP 里也显式列出）
            "d1_completed_map1",
            "d1_battles",
            "stuck_level_count",
            "overall_win_rate",
            "recent10_win_rate",
            "total_battles",
            "avg_session_gap_s",
            "special_tower_use_rate",
            "d3_active_days",
            # [MODIFIED_v2] daily summary 需要：top_stuck_map_id 的来源
            "first_stuck_map_id",  # [MODIFIED_v2]
        ]
    )

    available = set(pred_sdf.columns)
    use_cols = [c for c in required_cols if c in available]

    feature_df = pred_sdf.select(*use_cols, "churn_prob").toPandas()
    print(f"  待预警用户数: {len(feature_df):,}")

    # 3e. 规则引擎 + 综合预警
    results = []
    for _, row in feature_df.iterrows():
        user_row = row.to_dict()

        # 规则判断
        force_high, triggered, rule_descs = apply_rules(user_row)

        # 风险等级
        prob = user_row["churn_prob"]
        if force_high or prob >= HIGH_RISK_THRESHOLD:
            risk_level = "high"
            final_alert = 1
        elif prob >= MEDIUM_RISK_THRESHOLD:
            risk_level = "medium"
            final_alert = 0  # 中风险暂不触发干预，仅记录
        else:
            risk_level = "low"
            final_alert = 0

        # 流失原因
        # top_reasons = get_top_reasons(user_row, model, feature_names, rule_descs)  # [OLD-LGBM]
        top_reasons = get_top_reasons(user_row, rule_descs)  # [MODIFIED]

        results.append(
            {
                "user_id": int(user_row["user_id"]),
                "churn_prob": round(float(prob), 4),
                "risk_level": risk_level,
                "rule_trigger": json.dumps(triggered, ensure_ascii=False),
                "final_alert": final_alert,
                "top_reason_1": top_reasons[0] if len(top_reasons) > 0 else "",
                "top_reason_2": top_reasons[1] if len(top_reasons) > 1 else "",
                "top_reason_3": top_reasons[2] if len(top_reasons) > 2 else "",
                # "predict_dt"  : pd.Timestamp.today().strftime("%Y-%m-%d"),  # [OLD]
                "predict_dt": RUN_DT,  # [MODIFIED_v2]
                "model_version": "rf_v1",  # [MODIFIED] 标记模型版本
            }
        )

    result_df = pd.DataFrame(results)

    # 3f. 统计
    print("\n[RESULT] 预警结果统计:")
    print(f"  高风险 (high)  : {(result_df['risk_level']=='high').sum():,}")
    print(f"  中风险 (medium): {(result_df['risk_level']=='medium').sum():,}")
    print(f"  低风险 (low)   : {(result_df['risk_level']=='low').sum():,}")
    print(f"  触发预警       : {result_df['final_alert'].sum():,}")
    print(f"  平均流失概率   : {result_df['churn_prob'].mean():.4f}")

    # 高风险用户展示
    print("\n  高风险用户样例（前10条）:")
    cols = [
        "user_id",
        "churn_prob",
        "risk_level",
        "rule_trigger",
        "top_reason_1",
        "top_reason_2",
    ]
    print(
        result_df[result_df["risk_level"] == "high"][cols]
        .head(10)
        .to_string(index=False)
    )

    # 3g. 写出 CSV
    result_df.to_csv(
        f"{OUTPUT_DIR}/alert_result.csv", index=False, encoding="utf-8-sig"
    )
    print(f"\n  结果已保存: {OUTPUT_DIR}/alert_result.csv")

    # ───────────────────────────────────────────────────────────
    # [MODIFIED_v2] 先构造 Spark DataFrame（后续要写 Hive + MySQL）
    # ───────────────────────────────────────────────────────────
    result_sdf = spark.createDataFrame(result_df)

    # ───────────────────────────────────────────────────────────
    # [MODIFIED_v2] 写 Hive：td_churn.ads_user_churn_risk（按 DDL：PARTITIONED BY (dt STRING)）
    #   - 用 INSERT OVERWRITE + 静态分区 dt=RUN_DT，避免动态分区配置带来的不确定性
    #   - 字段顺序/类型严格按你提供的 DDL 来 cast
    # ───────────────────────────────────────────────────────────
    try:
        result_sdf.createOrReplaceTempView("tmp_ads_user_churn_risk_v2")
        spark.sql(
            f"""
            INSERT OVERWRITE TABLE td_churn.ads_user_churn_risk PARTITION (dt='{RUN_DT}')
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
                CAST(model_version AS STRING)   AS model_version
            FROM tmp_ads_user_churn_risk_v2
            """
        )
        print(f"  Hive → td_churn.ads_user_churn_risk 分区 dt={RUN_DT} 写出成功")
    except Exception as e:
        print(f"  [WARN] Hive 写 ads_user_churn_risk 跳过: {e}")

    # ───────────────────────────────────────────────────────────
    # 写 MySQL（ADS 层）——risk 表（原脚本逻辑保留）
    # ───────────────────────────────────────────────────────────
    try:
        # [MODIFIED_v3] 作为“快照表”使用：先清空再写入，避免 overwrite 触发 DROP
        mysql_exec_sql(
            spark, MYSQL_URL, MYSQL_USER, MYSQL_PASS,
            "DELETE FROM ads_user_churn_risk;")

        result_sdf.write \
            .format("jdbc") \
            .option("url", MYSQL_URL) \
            .option("dbtable", "ads_user_churn_risk") \
            .option("user", MYSQL_USER) \
            .option("password", MYSQL_PASS) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("append") \
            .save()
        print("  MySQL → ads_user_churn_risk 写出成功")
    except Exception as e:
        print(f"  [WARN] MySQL 写出跳过: {e}")

    # ───────────────────────────────────────────────────────────
    # 每日态势汇总（原脚本：只写 CSV）
    # ───────────────────────────────────────────────────────────
    # summary = {
    #     "stat_date"           : pd.Timestamp.today().strftime("%Y-%m-%d"),
    #     "total_active_users"  : len(result_df),
    #     "high_risk_count"     : int((result_df["risk_level"] == "high").sum()),
    #     "medium_risk_count"   : int((result_df["risk_level"] == "medium").sum()),
    #     "low_risk_count"      : int((result_df["risk_level"] == "low").sum()),
    #     "high_risk_rate"      : round((result_df["risk_level"] == "high").mean(), 4),
    #     "avg_churn_prob"      : round(result_df["churn_prob"].mean(), 4),
    # }
    # pd.DataFrame([summary]).to_csv(
    #     f"{OUTPUT_DIR}/daily_summary.csv", index=False, encoding="utf-8-sig"
    # )
    # print(f"  日态势汇总已保存: {OUTPUT_DIR}/daily_summary.csv")
    #
    # [OLD] ↑↑↑ 原 daily_summary 只包含 7 个字段，且只写 CSV
    # ───────────────────────────────────────────────────────────

    # ───────────────────────────────────────────────────────────
    # [MODIFIED_v2] daily_summary：补齐大屏字段 + 同时写 Hive & MySQL
    #   你描述的结构包含：
    #     top_stuck_map_id, d1_no_tutorial_count
    #   这里基于 feature_df（含 first_stuck_map_id / d1_completed_map1）计算
    # ───────────────────────────────────────────────────────────
    # 1) 首日未完成新手引导用户数（按 d1_completed_map1==0）
    if "d1_completed_map1" in feature_df.columns:
        d1_no_tutorial_count = int((feature_df["d1_completed_map1"].fillna(1) == 0).sum())
    else:
        d1_no_tutorial_count = 0

    # 2) 当日卡关最多的地图ID（first_stuck_map_id 的众数/最高频）
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
                top_stuck_map_id_2 = int(vc.index[1])  # 第二高卡关地图

    # 3) 新增汇总指标（需新特征字段，旧版 parquet 中若不存在则返回 0）
    avg_battles_per_user = float(round(feature_df["total_battles"].mean(), 2)) \
        if "total_battles" in feature_df.columns else 0.0
    stuck_user_count = int((feature_df["stuck_level_count"] > 0).sum()) \
        if "stuck_level_count" in feature_df.columns else 0
    narrow_win_rate_overall = float(round(feature_df["narrow_win_rate"].mean(), 4)) \
        if "narrow_win_rate" in feature_df.columns else 0.0

    summary = {
        "stat_date": RUN_DT,
        "total_active_users": int(len(result_df)),
        "high_risk_count": int((result_df["risk_level"] == "high").sum()),
        "medium_risk_count": int((result_df["risk_level"] == "medium").sum()),
        "low_risk_count": int((result_df["risk_level"] == "low").sum()),
        "high_risk_rate": float(round((result_df["risk_level"] == "high").mean(), 4)),
        "avg_churn_prob": float(round(result_df["churn_prob"].mean(), 4)),
        "top_stuck_map_id": int(top_stuck_map_id),
        "d1_no_tutorial_count": int(d1_no_tutorial_count),
        # 新增扩展字段
        "avg_battles_per_user": avg_battles_per_user,
        "stuck_user_count": stuck_user_count,
        "narrow_win_rate_overall": narrow_win_rate_overall,
        "top_stuck_map_id_2": int(top_stuck_map_id_2),
    }

    pd.DataFrame([summary]).to_csv(
        f"{OUTPUT_DIR}/daily_summary.csv", index=False, encoding="utf-8-sig"
    )
    print(f"  日态势汇总已保存: {OUTPUT_DIR}/daily_summary.csv")

    # [MODIFIED_v2] 写 Hive daily summary（表是否分区不确定，提供“先按分区写→失败再无分区写”的兜底）
    try:
        summary_sdf = spark.createDataFrame([summary])
        summary_sdf.createOrReplaceTempView("tmp_ads_daily_churn_summary_v2")

        # 先尝试：如果 Hive 表也按 dt 分区（很多项目会这样），走这个
        try:
            spark.sql(
                f"""
                INSERT OVERWRITE TABLE td_churn.ads_daily_churn_summary PARTITION (dt='{RUN_DT}')
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
            )
            print(f"  Hive → td_churn.ads_daily_churn_summary 分区 dt={RUN_DT} 写出成功")
        except Exception as e_partition:
            # 再兜底：如果该表不分区，用普通 INSERT OVERWRITE
            spark.sql(
                """
                INSERT OVERWRITE TABLE td_churn.ads_daily_churn_summary
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
            )
            print("  Hive → td_churn.ads_daily_churn_summary（非分区表）写出成功")
    except Exception as e:
        print(f"  [WARN] Hive 写 ads_daily_churn_summary 跳过: {e}")

    # [MODIFIED_v2] 写 MySQL daily summary（与 Hive 字段一致）
    try:
        # [MODIFIED_v3] 幂等：先删当天，再 append，避免重复行
        mysql_exec_sql(
            spark, MYSQL_URL, MYSQL_USER, MYSQL_PASS,
            f"DELETE FROM ads_daily_churn_summary WHERE stat_date = '{RUN_DT}';")

        summary_sdf_mysql = spark.createDataFrame([summary])
        summary_sdf_mysql.write \
            .format("jdbc") \
            .option("url", MYSQL_URL) \
            .option("dbtable", "ads_daily_churn_summary") \
            .option("user", MYSQL_USER) \
            .option("password", MYSQL_PASS) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("append") \
            .save()
        print("  MySQL → ads_daily_churn_summary 写出成功")
    except Exception as e:
        print(f"  [WARN] MySQL 写 ads_daily_churn_summary 跳过: {e}")

    # ───────────────────────────────────────────────────────────
    # [NEW] 关卡卡关热力图（ads_map_churn_hotspot）
    #   从 DWD 层战斗明细聚合各关卡的失败率、险胜率、高风险玩家数等指标
    # ───────────────────────────────────────────────────────────
    try:
        dwd = spark.table(f"{HIVE_DB}.dwd_battle_detail")
        map_hotspot_sdf = dwd.groupBy("map_id", "difficulty_tier").agg(
            F.count("*").alias("total_attempts"),
            (F.lit(1) - F.mean("battle_result")).alias("fail_rate"),
            F.mean("base_hp_ratio").alias("avg_hp_ratio"),
            F.mean("used_special_tower").alias("help_usage_rate"),
            F.countDistinct("user_id").alias("player_count"),
            F.mean("map_clear_rate").alias("map_clear_rate"),
        )

        # 关联高风险玩家数（已判定为高风险的玩家）
        high_risk_uid = result_sdf.filter(F.col("risk_level") == "high").select("user_id")
        high_risk_per_map = (
            dwd.join(high_risk_uid, on="user_id", how="inner")
               .groupBy("map_id")
               .agg(F.countDistinct("user_id").alias("high_risk_player_count"))
        )
        map_hotspot_sdf = (
            map_hotspot_sdf
            .join(high_risk_per_map, on="map_id", how="left")
            .fillna({"high_risk_player_count": 0})
            .withColumn("stat_date", F.lit(RUN_DT))
        )

        # 写 Hive
        try:
            map_hotspot_sdf.createOrReplaceTempView("tmp_ads_map_churn_hotspot")
            spark.sql(f"""
                INSERT OVERWRITE TABLE {HIVE_DB}.ads_map_churn_hotspot PARTITION (dt='{RUN_DT}')
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
            print(f"  Hive → {HIVE_DB}.ads_map_churn_hotspot 分区 dt={RUN_DT} 写出成功")
        except Exception as e_hive:
            print(f"  [WARN] Hive 写 ads_map_churn_hotspot 跳过: {e_hive}")

        # 写 MySQL
        try:
            mysql_exec_sql(
                spark, MYSQL_URL, MYSQL_USER, MYSQL_PASS,
                f"DELETE FROM ads_map_churn_hotspot WHERE stat_date = '{RUN_DT}';")
            map_hotspot_sdf.write \
                .format("jdbc") \
                .option("url", MYSQL_URL) \
                .option("dbtable", "ads_map_churn_hotspot") \
                .option("user", MYSQL_USER) \
                .option("password", MYSQL_PASS) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .mode("append") \
                .save()
            print("  MySQL → ads_map_churn_hotspot 写出成功")
        except Exception as e_mysql:
            print(f"  [WARN] MySQL 写 ads_map_churn_hotspot 跳过: {e_mysql}")

    except Exception as e:
        print(f"  [WARN] 关卡卡关热力图聚合跳过: {e}")

    spark.stop()
    print("\n√√√ 规则引擎预警完成")


if __name__ == "__main__":
    run_alert_pipeline()

