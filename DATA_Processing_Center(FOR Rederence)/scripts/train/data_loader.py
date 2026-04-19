"""
train.data_loader
=================
Load feature Parquet files, apply EDA feature whitelist,
compute class imbalance weight, and provide time-window feature filtering.
"""

import os

from pyspark.sql import functions as F

from config_env import env, env_int

PROJECT_DIR  = env("TD_CHURN_PROJECT_DIR", "/home/hadoop/td_churn_project")
FEATURE_DIR  = env("TD_CHURN_ETL_OUTPUT_DIR", f"{PROJECT_DIR}/etl_output_v2")
EDA_DIR      = env("TD_CHURN_EDA_OUTPUT_DIR", f"{PROJECT_DIR}/eda_output")

NON_FEAT = {"user_id", "label", "split", "first_stuck_map_id"}

D1_PREFIX = "d1_"
D3_PREFIX = "d3_"
D7_PREFIX = "d7_"


def load_kept_features():
    """Read EDA-stage feature whitelist from kept_features.txt."""
    path = os.path.join(EDA_DIR, "kept_features.txt")
    if not os.path.exists(path):
        print("[WARN] kept_features.txt not found; using all features")
        return None
    with open(path) as f:
        feats = [line.strip() for line in f if line.strip()]
    print(f"[FEATURE] Loaded {len(feats)} features from EDA whitelist")
    return feats


def calc_metrics(name, y_true, y_pred, y_prob, window="full"):
    """Compute all evaluation metrics and print a summary line."""
    from sklearn.metrics import (
        roc_auc_score, f1_score, accuracy_score,
        precision_score, recall_score, confusion_matrix,
    )
    cm = confusion_matrix(y_true, y_pred)
    tn, fp, fn, tp = cm.ravel()
    metrics = {
        "model":     name,
        "window":    window,
        "auc":       round(roc_auc_score(y_true, y_prob), 4),
        "f1":        round(f1_score(y_true, y_pred), 4),
        "accuracy":  round(accuracy_score(y_true, y_pred), 4),
        "precision": round(precision_score(y_true, y_pred), 4),
        "recall":    round(recall_score(y_true, y_pred), 4),
        "tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp),
    }
    print(f"  [{window}] AUC={metrics['auc']:.4f}  F1={metrics['f1']:.4f}  "
          f"Acc={metrics['accuracy']:.4f}  P={metrics['precision']:.4f}  "
          f"R={metrics['recall']:.4f}")
    print(f"  Confusion: TN={tn} FP={fp} FN={fn} TP={tp}")
    return metrics


def get_window_features(feat_cols, window):
    """
    Filter feature columns by time window.

    window="full" -> all features
    window="D3"   -> drop d7_ prefix (simulate first-3-day prediction)
    window="D1"   -> keep d1_ + non-window features only
    """
    if window == "full":
        return feat_cols
    elif window == "D3":
        return [c for c in feat_cols if not c.startswith(D7_PREFIX)]
    elif window == "D1":
        return [c for c in feat_cols
                if not c.startswith(D3_PREFIX) and not c.startswith(D7_PREFIX)]
    return feat_cols


def load_data(spark):
    """
    Load train/dev Parquet, determine feature columns, and compute
    class imbalance weight.

    Returns (train_sdf, dev_sdf, feat_cols, scale_pos_weight).
    """
    print("\n[DATA] Loading feature tables ...")
    train_sdf = spark.read.parquet(f"{FEATURE_DIR}/features_train.parquet")
    dev_sdf   = spark.read.parquet(f"{FEATURE_DIR}/features_dev.parquet")

    numeric_types = {"double", "float", "integer", "long"}
    all_feat = [c for c in train_sdf.columns
                if c not in NON_FEAT
                and train_sdf.schema[c].dataType.typeName() in numeric_types]

    kept = load_kept_features()
    feat_cols = [c for c in all_feat if kept is None or c in kept]

    pos = train_sdf.filter(F.col("label") == 1).count()
    neg = train_sdf.filter(F.col("label") == 0).count()
    scale_pos_weight = neg / pos
    print(f"  Train: {pos + neg:,} users  pos:neg = 1:{scale_pos_weight:.2f}")
    print(f"  Dev:   {dev_sdf.count():,} users")
    print(f"  Feature count: {len(feat_cols)}")

    return train_sdf, dev_sdf, feat_cols, scale_pos_weight
