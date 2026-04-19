"""
train.gbdt_models
=================
LightGBM and XGBoost training with StratifiedKFold cross-validation,
plus experiment result summarisation.
"""

import time, json
import numpy as np
import pandas as pd
import lightgbm as lgb
import xgboost as xgb

from sklearn.metrics import roc_auc_score, roc_curve, auc
from sklearn.model_selection import StratifiedKFold

from config_env import env, env_int
from train.data_loader import calc_metrics

MODEL_DIR   = env("TD_CHURN_MODEL_DIR",
                   env("TD_CHURN_PROJECT_DIR", "/home/hadoop/td_churn_project")
                   + "/models")
RESULT_DIR  = env("TD_CHURN_EXPERIMENT_DIR",
                   env("TD_CHURN_PROJECT_DIR", "/home/hadoop/td_churn_project")
                   + "/experiment_results")
RANDOM_SEED = env_int("TD_CHURN_RANDOM_SEED", 42)
CV_FOLDS    = env_int("TD_CHURN_CV_FOLDS", 5)


def train_lightgbm(X_train, y_train, X_dev, y_dev, feat_cols,
                    scale_pos_weight, window="full"):
    """Train LightGBM with StratifiedKFold CV. Returns (metrics, y_dev, y_prob)."""
    print(f"\n[LGBM] LightGBM [{window}] ...")
    t0 = time.time()

    params = {
        "objective":         "binary",
        "metric":            "auc",
        "learning_rate":     0.05,
        "num_leaves":        63,
        "max_depth":         -1,
        "min_child_samples": 20,
        "feature_fraction":  0.8,
        "bagging_fraction":  0.8,
        "bagging_freq":      5,
        "lambda_l1":         0.1,
        "lambda_l2":         0.1,
        "scale_pos_weight":  scale_pos_weight,
        "seed":              RANDOM_SEED,
        "verbose":           -1,
    }

    skf     = StratifiedKFold(n_splits=CV_FOLDS, shuffle=True, random_state=RANDOM_SEED)
    cv_aucs = []
    for fold, (tr_idx, va_idx) in enumerate(skf.split(X_train, y_train), 1):
        X_tr, X_va = X_train.iloc[tr_idx], X_train.iloc[va_idx]
        y_tr, y_va = y_train.iloc[tr_idx], y_train.iloc[va_idx]
        dtr = lgb.Dataset(X_tr, label=y_tr)
        dva = lgb.Dataset(X_va, label=y_va, reference=dtr)
        m = lgb.train(params, dtr, num_boost_round=500,
                      valid_sets=[dva],
                      callbacks=[lgb.early_stopping(30, verbose=False),
                                 lgb.log_evaluation(-1)])
        fold_auc = roc_auc_score(y_va, m.predict(X_va))
        cv_aucs.append(fold_auc)
        print(f"  Fold {fold}: AUC={fold_auc:.4f}")

    print(f"  CV AUC: {np.mean(cv_aucs):.4f} +/- {np.std(cv_aucs):.4f}")

    # Final model on full train set
    dtrain = lgb.Dataset(X_train, label=y_train, feature_name=list(feat_cols))
    ddev   = lgb.Dataset(X_dev,   label=y_dev, reference=dtrain)
    final_model = lgb.train(
        params, dtrain, num_boost_round=1000,
        valid_sets=[ddev],
        callbacks=[lgb.early_stopping(50, verbose=False),
                   lgb.log_evaluation(100)]
    )

    y_prob = final_model.predict(X_dev)
    y_pred = (y_prob >= 0.5).astype(int)
    metrics = calc_metrics("LightGBM", y_dev, y_pred, y_prob, window)
    metrics["cv_auc_mean"]    = round(np.mean(cv_aucs), 4)
    metrics["cv_auc_std"]     = round(np.std(cv_aucs), 4)
    metrics["best_iteration"] = final_model.best_iteration
    metrics["train_time_s"]   = round(time.time() - t0, 1)

    imp_df = pd.DataFrame({
        "feature":    final_model.feature_name(),
        "importance": final_model.feature_importance(importance_type="gain"),
    }).sort_values("importance", ascending=False)
    metrics["feature_importance"] = imp_df.values.tolist()[:20]
    print(f"  Top5 features: {imp_df['feature'].head(5).tolist()}")
    print(f"  Best iteration: {final_model.best_iteration}  "
          f"Time: {metrics['train_time_s']}s")

    if window == "full":
        final_model.save_model(f"{MODEL_DIR}/lightgbm_full.txt")

    return metrics, y_dev, y_prob


def train_xgboost(X_train, y_train, X_dev, y_dev, feat_cols,
                   scale_pos_weight, window="full"):
    """Train XGBoost with StratifiedKFold CV. Returns (metrics, y_dev, y_prob)."""
    print(f"\n[XGB] XGBoost [{window}] ...")
    t0 = time.time()

    params = {
        "objective":         "binary:logistic",
        "eval_metric":       "auc",
        "learning_rate":     0.05,
        "max_depth":         6,
        "min_child_weight":  5,
        "subsample":         0.8,
        "colsample_bytree":  0.8,
        "reg_alpha":         0.1,
        "reg_lambda":        1.0,
        "scale_pos_weight":  scale_pos_weight,
        "seed":              RANDOM_SEED,
        "verbosity":         0,
    }

    skf     = StratifiedKFold(n_splits=CV_FOLDS, shuffle=True, random_state=RANDOM_SEED)
    cv_aucs = []
    for fold, (tr_idx, va_idx) in enumerate(skf.split(X_train, y_train), 1):
        X_tr, X_va = X_train.iloc[tr_idx], X_train.iloc[va_idx]
        y_tr, y_va = y_train.iloc[tr_idx], y_train.iloc[va_idx]
        dtr = xgb.DMatrix(X_tr, label=y_tr)
        dva = xgb.DMatrix(X_va, label=y_va)
        m   = xgb.train(params, dtr, num_boost_round=500,
                        evals=[(dva, "val")], early_stopping_rounds=30,
                        verbose_eval=False)
        fold_auc = roc_auc_score(y_va, m.predict(dva))
        cv_aucs.append(fold_auc)
        print(f"  Fold {fold}: AUC={fold_auc:.4f}")

    print(f"  CV AUC: {np.mean(cv_aucs):.4f} +/- {np.std(cv_aucs):.4f}")

    dtrain = xgb.DMatrix(X_train, label=y_train)
    ddev   = xgb.DMatrix(X_dev,   label=y_dev)
    final_model = xgb.train(
        params, dtrain, num_boost_round=1000,
        evals=[(ddev, "dev")], early_stopping_rounds=50,
        verbose_eval=100,
    )

    y_prob = final_model.predict(ddev)
    y_pred = (y_prob >= 0.5).astype(int)
    metrics = calc_metrics("XGBoost", y_dev, y_pred, y_prob, window)
    metrics["cv_auc_mean"]    = round(np.mean(cv_aucs), 4)
    metrics["cv_auc_std"]     = round(np.std(cv_aucs), 4)
    metrics["best_iteration"] = final_model.best_iteration
    metrics["train_time_s"]   = round(time.time() - t0, 1)

    score_dict = final_model.get_score(importance_type="gain")
    imp_df = pd.DataFrame(list(score_dict.items()),
                          columns=["feature", "importance"]) \
               .sort_values("importance", ascending=False)
    metrics["feature_importance"] = imp_df.values.tolist()[:20]

    if window == "full":
        final_model.save_model(f"{MODEL_DIR}/xgboost_full.json")

    return metrics, y_dev, y_prob


def summarize_results(all_results, roc_data):
    """Print comparison table, save CSVs, ROC data, and best model name."""
    import os
    os.makedirs(RESULT_DIR, exist_ok=True)

    print("\n" + "=" * 70)
    print("Model Comparison Summary (dev set)")
    print("=" * 70)

    summary_cols = ["model", "window", "auc", "f1", "accuracy",
                    "precision", "recall", "cv_auc_mean", "cv_auc_std",
                    "best_iteration", "train_time_s", "tn", "fp", "fn", "tp"]
    rows = [{k: m.get(k, "") for k in summary_cols} for m in all_results]

    df = pd.DataFrame(rows).sort_values(["window", "auc"], ascending=[True, False])
    print(df.to_string(index=False))
    df.to_csv(f"{RESULT_DIR}/model_comparison_full.csv", index=False)

    # Feature importance per full-window model
    for m in all_results:
        if "feature_importance" in m and m["window"] == "full":
            fi_df = pd.DataFrame(m["feature_importance"],
                                 columns=["feature", "importance"])
            fname = f"feat_imp_{m['model'].lower()}.csv"
            fi_df.to_csv(f"{RESULT_DIR}/{fname}", index=False)

    # Best full-window model
    full_df = df[df["window"] == "full"].sort_values("auc", ascending=False)
    best = full_df.iloc[0]
    print(f"\nBest full model: {best['model']}  "
          f"AUC={best['auc']:.4f}  F1={best['f1']:.4f}")

    # Time-window experiment summary (LightGBM)
    print("\nTime-window experiment (LightGBM):")
    lgbm_df = df[df["model"] == "LightGBM"][["window", "auc", "f1"]]
    print(lgbm_df.to_string(index=False))

    # ROC curve data for plotting
    roc_export = {}
    for model_name, (yt, yp) in roc_data.items():
        fpr, tpr, _ = roc_curve(np.array(yt), np.array(yp))
        roc_export[model_name] = {
            "fpr": fpr.tolist(),
            "tpr": tpr.tolist(),
            "auc": round(auc(fpr, tpr), 4),
        }
    with open(f"{RESULT_DIR}/roc_data.json", "w") as f:
        json.dump(roc_export, f)
    print(f"\nROC data saved: {RESULT_DIR}/roc_data.json")

    with open(f"{RESULT_DIR}/best_model.txt", "w") as f:
        f.write(best["model"])
    print(f"All results saved to: {RESULT_DIR}/")
