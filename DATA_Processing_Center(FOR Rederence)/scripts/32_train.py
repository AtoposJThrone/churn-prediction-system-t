"""
32_train.py
===========
Model training & comparison entry point.

Trains 4 models across 3 time windows:
  - LightGBM + XGBoost: full / D3 / D1 windows (pandas driver, StratifiedKFold)
  - RandomForest + GBT: full window only (Spark MLlib, CrossValidator)

Logic is delegated to:
  - train.data_loader   : feature loading, EDA whitelist, metrics
  - train.spark_models  : RF, GBT, Spark Pipeline, sample weighting
  - train.gbdt_models   : LightGBM, XGBoost, result summarisation

Run:
  spark-submit --master yarn --deploy-mode client 32_train.py
"""

import os
import warnings

from config_env import env, env_int

from common.spark_helper import build_spark_session
from train.data_loader import load_data, get_window_features
from train.spark_models import (
    build_spark_pipeline, add_sample_weight,
    train_random_forest, train_gbt,
)
from train.gbdt_models import (
    train_lightgbm, train_xgboost, summarize_results,
)

warnings.filterwarnings("ignore")

MODEL_DIR  = env("TD_CHURN_MODEL_DIR",
                  env("TD_CHURN_PROJECT_DIR", "/home/hadoop/td_churn_project")
                  + "/models")
os.makedirs(MODEL_DIR, exist_ok=True)

all_results = []


def main():
    spark = build_spark_session("TD_Churn_ModelTraining", enable_hive=False,
                                shuffle_partitions=100)

    train_sdf, dev_sdf, feat_cols, spw = load_data(spark)

    # Convert to pandas once for GBDT models
    def to_pandas(sdf, cols):
        pdf = sdf.select(["label"] + cols).toPandas()
        X = pdf[cols].fillna(pdf[cols].mean())
        y = pdf["label"].astype(int)
        return X, y

    print("\n[COLLECT] Converting to pandas ...")
    X_train_all, y_train = to_pandas(train_sdf, feat_cols)
    X_dev_all,   y_dev   = to_pandas(dev_sdf,   feat_cols)

    # Three time-window experiments
    windows = {
        "full": feat_cols,
        "D3":   get_window_features(feat_cols, "D3"),
        "D1":   get_window_features(feat_cols, "D1"),
    }
    print("\n[EXPERIMENT] Time-window design:")
    for w, cols in windows.items():
        print(f"  {w}: {len(cols)} features")

    roc_data = {}

    # LightGBM + XGBoost for each window
    for window, wcols in windows.items():
        X_tr = X_train_all[wcols]
        X_dv = X_dev_all[wcols]

        lgbm_m, yt, yp = train_lightgbm(X_tr, y_train, X_dv, y_dev,
                                          wcols, spw, window)
        all_results.append(lgbm_m)
        if window == "full":
            roc_data["LightGBM"] = (yt, yp)

        xgb_m, yt, yp = train_xgboost(X_tr, y_train, X_dv, y_dev,
                                        wcols, spw, window)
        all_results.append(xgb_m)
        if window == "full":
            roc_data["XGBoost"] = (yt, yp)

    # RF / GBT: full window only (Spark distributed)
    print("\n[SPARK] Building Spark Pipeline (full features) ...")
    train_weighted = add_sample_weight(train_sdf, spw)
    dev_weighted   = add_sample_weight(dev_sdf,   spw)
    pipeline_model = build_spark_pipeline(train_weighted, feat_cols)

    PIPELINE_DIR = f"{MODEL_DIR}/spark_feature_pipeline"
    pipeline_model.write().overwrite().save(PIPELINE_DIR)
    print(f"[SAVE] Pipeline saved to: {PIPELINE_DIR}")

    train_proc = pipeline_model.transform(train_weighted)
    dev_proc   = pipeline_model.transform(dev_weighted)

    rf_m, yt, yp = train_random_forest(train_proc, dev_proc, feat_cols, spw)
    all_results.append(rf_m)
    roc_data["RandomForest"] = (yt, yp)

    gbt_m, yt, yp = train_gbt(train_proc, dev_proc, feat_cols, spw)
    all_results.append(gbt_m)
    roc_data["GBT"] = (yt, yp)

    summarize_results(all_results, roc_data)

    spark.stop()
    print("\nModel training complete.")


if __name__ == "__main__":
    main()
