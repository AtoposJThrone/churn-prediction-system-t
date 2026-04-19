"""
alert.scoring
=============
Load Spark Pipeline + RF model, score users, then apply rules
on the driver side to produce the final alert DataFrame.
"""

import json
import pandas as pd

from pyspark.sql import functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler, Imputer
from pyspark.ml.classification import RandomForestClassificationModel

try:
    from pyspark.ml.functions import vector_to_array
except Exception:
    from pyspark.ml.linalg import VectorUDT

    @F.udf("array<double>")
    def vector_to_array(v):
        if v is None:
            return [None, None]
        try:
            return v.toArray().tolist()
        except Exception:
            return [None, None]

from config_env import env, env_int
from alert.rules import (
    apply_rules, get_top_reasons, FEATURE_REASON_MAP,
)

PROJECT_DIR    = env("TD_CHURN_PROJECT_DIR", "/home/hadoop/td_churn_project")
MODEL_DIR      = env("TD_CHURN_MODEL_DIR", f"{PROJECT_DIR}/models")
MODEL_PATH     = env("TD_CHURN_RF_MODEL_PATH", f"{MODEL_DIR}/rf_model")
PIPELINE_PATH  = env("TD_CHURN_PIPELINE_MODEL_PATH", f"{MODEL_DIR}/spark_feature_pipeline")
FEATURE_DIR    = env("TD_CHURN_ETL_OUTPUT_DIR", f"{PROJECT_DIR}/etl_output_v2")

HIGH_RISK_THRESHOLD   = 0.66
MEDIUM_RISK_THRESHOLD = 0.47

NON_FEAT = {"user_id", "label", "split", "first_stuck_map_id"}


def _build_pipeline_fallback(sdf):
    """Emergency: fit a pipeline on inference data when saved pipeline is missing."""
    numeric_types = {"double", "float", "integer", "long"}
    feat_cols = [c for c in sdf.columns
                 if c not in NON_FEAT
                 and sdf.schema[c].dataType.typeName() in numeric_types]
    imp_out   = [f"{c}_imp" for c in feat_cols]
    imputer   = Imputer(inputCols=feat_cols, outputCols=imp_out, strategy="mean")
    assembler = VectorAssembler(inputCols=imp_out, outputCol="features_raw",
                                handleInvalid="keep")
    scaler    = StandardScaler(inputCol="features_raw", outputCol="features",
                               withMean=True, withStd=True)
    pipeline  = Pipeline(stages=[imputer, assembler, scaler])
    return pipeline.fit(sdf), feat_cols


def score_users(spark, run_dt: str):
    """
    Load features, run Spark Pipeline + RF model, apply rules,
    and return (result_df, feature_df, result_sdf).

    result_df  : pandas DataFrame with per-user alert results
    feature_df : pandas DataFrame with raw feature values (for summary)
    result_sdf : Spark DataFrame version of result_df
    """
    print("[ALERT] Loading feature table ...")
    test_sdf = spark.read.parquet(f"{FEATURE_DIR}/features_test.parquet")
    all_sdf  = test_sdf

    # Load pipeline
    print("[ALERT] Loading Spark Pipeline & RF model ...")
    try:
        pipeline_model = PipelineModel.load(PIPELINE_PATH)
        print(f"  Pipeline loaded: {PIPELINE_PATH}")
    except Exception as e:
        print(f"  [WARN] Pipeline load failed, fitting fallback: {e}")
        print("  WARNING: fallback Imputer uses inference data stats, not training stats!")
        pipeline_model, _ = _build_pipeline_fallback(all_sdf)

    rf_model = RandomForestClassificationModel.load(MODEL_PATH)
    print(f"  RF model loaded: {MODEL_PATH}")

    # Score
    proc_sdf = pipeline_model.transform(all_sdf)
    pred_sdf = rf_model.transform(proc_sdf)
    pred_sdf = pred_sdf.withColumn(
        "churn_prob", vector_to_array(F.col("probability"))[1].cast("double")
    )

    # Collect only needed columns to driver
    required_cols = (
        {"user_id"} | set(FEATURE_REASON_MAP.keys()) |
        {"d1_completed_map1", "d1_battles", "stuck_level_count",
         "overall_win_rate", "recent10_win_rate", "total_battles",
         "avg_session_gap_s", "special_tower_use_rate", "d3_active_days",
         "first_stuck_map_id"}
    )
    use_cols = [c for c in required_cols if c in set(pred_sdf.columns)]
    feature_df = pred_sdf.select(*use_cols, "churn_prob").toPandas()
    print(f"  Users to evaluate: {len(feature_df):,}")

    # Apply rules + risk level
    results = []
    for _, row in feature_df.iterrows():
        user_row = row.to_dict()
        force_high, triggered, rule_descs = apply_rules(user_row)
        prob = user_row["churn_prob"]

        if force_high or prob >= HIGH_RISK_THRESHOLD:
            risk_level, final_alert = "high", 1
        elif prob >= MEDIUM_RISK_THRESHOLD:
            risk_level, final_alert = "medium", 0
        else:
            risk_level, final_alert = "low", 0

        top_reasons = get_top_reasons(user_row, rule_descs)
        results.append({
            "user_id":       int(user_row["user_id"]),
            "churn_prob":    round(float(prob), 4),
            "risk_level":    risk_level,
            "rule_trigger":  json.dumps(triggered, ensure_ascii=False),
            "final_alert":   final_alert,
            "top_reason_1":  top_reasons[0] if len(top_reasons) > 0 else "",
            "top_reason_2":  top_reasons[1] if len(top_reasons) > 1 else "",
            "top_reason_3":  top_reasons[2] if len(top_reasons) > 2 else "",
            "predict_dt":    run_dt,
            "model_version": "rf_v1",
        })

    result_df = pd.DataFrame(results)

    # Print summary
    print("\n[RESULT] Alert summary:")
    print(f"  High risk : {(result_df['risk_level']=='high').sum():,}")
    print(f"  Medium    : {(result_df['risk_level']=='medium').sum():,}")
    print(f"  Low       : {(result_df['risk_level']=='low').sum():,}")
    print(f"  Alerts    : {result_df['final_alert'].sum():,}")
    print(f"  Avg prob  : {result_df['churn_prob'].mean():.4f}")

    cols = ["user_id", "churn_prob", "risk_level", "rule_trigger",
            "top_reason_1", "top_reason_2"]
    print("\n  High-risk sample (top 10):")
    print(result_df[result_df["risk_level"] == "high"][cols]
          .head(10).to_string(index=False))

    result_sdf = spark.createDataFrame(result_df)
    return result_df, feature_df, result_sdf
