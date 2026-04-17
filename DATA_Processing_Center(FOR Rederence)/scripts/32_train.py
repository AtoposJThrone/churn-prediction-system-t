"""
32_train.py
===========
阶段二：模型训练与对比实验（修订版）

对比原训练方案的改动
---------------------------------
1. 读取 31_eda.py 输出的 kept_features.txt，自动剔除冗余特征
2. 处理类别不平衡（LightGBM/XGBoost 用 scale_pos_weight，RF/GBT 用 weightCol）
3. 修复特征 Pipeline 中多余的列重命名逻辑
4. 当前主流程运行三组时间窗口实验：全量特征 / 仅 D3 特征 / 仅 D1 特征
     — D3 通过去除 d7_ 前缀特征模拟前 3 天预测；D1 通过保留 d1_ 和非 d3_/d7_ 特征模拟首日预测
5. 每个模型输出完整评估报告（含混淆矩阵、分类报告）
6. 保存最优模型和 Pipeline，供 rule_engine.py 直接加载

运行方式：
    python3 32_train.py

  或通过 spark-submit 提交（RF/GBT 走 Spark 分布式）：
  spark-submit \\
    --master yarn \\
    --deploy-mode client \\
    --driver-memory 2g \\
    --executor-memory 1g \\
    --num-executors 2 \\
        32_train.py
"""

import os, json, time
import warnings
import numpy  as np
import pandas as pd
import lightgbm as lgb
import xgboost  as xgb

from sklearn.metrics import (
    roc_auc_score, f1_score, accuracy_score,
    precision_score, recall_score, classification_report,
    confusion_matrix, roc_curve, auc
)
from sklearn.model_selection import StratifiedKFold

from pyspark.sql    import SparkSession
from pyspark.sql    import functions as F
from pyspark.ml     import Pipeline
from pyspark.ml.feature   import VectorAssembler, StandardScaler, Imputer
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation    import BinaryClassificationEvaluator
from pyspark.ml.tuning        import CrossValidator, ParamGridBuilder

from config_env import env, env_int

warnings.filterwarnings("ignore")

# ─────────────────────────── 配置 ──────────────────────────────
PROJECT_DIR  = env("TD_CHURN_PROJECT_DIR", "/home/hadoop/td_churn_project")
FEATURE_DIR  = env("TD_CHURN_ETL_OUTPUT_DIR", f"{PROJECT_DIR}/etl_output_v2")
EDA_DIR      = env("TD_CHURN_EDA_OUTPUT_DIR", f"{PROJECT_DIR}/eda_output")
MODEL_DIR    = env("TD_CHURN_MODEL_DIR", f"{PROJECT_DIR}/models")
RESULT_DIR   = env("TD_CHURN_EXPERIMENT_DIR", f"{PROJECT_DIR}/experiment_results")
RANDOM_SEED  = env_int("TD_CHURN_RANDOM_SEED", 42)
CV_FOLDS     = env_int("TD_CHURN_CV_FOLDS", 5)
SHUFFLE_PARTITIONS = str(env_int("TD_CHURN_SPARK_SHUFFLE_PARTITIONS", 100))

os.makedirs(MODEL_DIR,  exist_ok=True)
os.makedirs(RESULT_DIR, exist_ok=True)

NON_FEAT = {"user_id", "label", "split", "first_stuck_map_id"}

# D1/D3/D7 特征前缀（时间窗口实验用）
D1_PREFIX = "d1_"
D3_PREFIX = "d3_"
D7_PREFIX = "d7_"

# ─────────────────────────── Spark ─────────────────────────────
spark = SparkSession.builder \
    .appName("TD_Churn_ModelTraining") \
    .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

all_results = []   # 收集所有实验结果

# ══════════════════════════════════════════════════════════════
# 工具函数
# ══════════════════════════════════════════════════════════════
def load_kept_features():
    """读取 EDA 阶段输出的特征白名单"""
    path = os.path.join(EDA_DIR, "kept_features.txt")
    if not os.path.exists(path):
        print("[WARN] 未找到 kept_features.txt，使用所有特征")
        return None
    with open(path) as f:
        feats = [l.strip() for l in f if l.strip()]
    print(f"[FEATURE] 读取 EDA 筛选后特征: {len(feats)} 个")
    return feats


def calc_metrics(name, y_true, y_pred, y_prob, window="全量"):
    """统一计算所有评估指标"""
    cm = confusion_matrix(y_true, y_pred)
    tn, fp, fn, tp = cm.ravel()
    metrics = {
        "model"    : name,
        "window"   : window,
        "auc"      : round(roc_auc_score(y_true, y_prob), 4),
        "f1"       : round(f1_score(y_true, y_pred), 4),
        "accuracy" : round(accuracy_score(y_true, y_pred), 4),
        "precision": round(precision_score(y_true, y_pred), 4),
        "recall"   : round(recall_score(y_true, y_pred), 4),
        "tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp),
    }
    print(f"  [{window}] AUC={metrics['auc']:.4f}  F1={metrics['f1']:.4f}  "
          f"Acc={metrics['accuracy']:.4f}  P={metrics['precision']:.4f}  "
          f"R={metrics['recall']:.4f}")
    print(f"  混淆矩阵: TN={tn} FP={fp} FN={fn} TP={tp}")
    return metrics


def get_window_features(feat_cols, window):
    """
    按时间窗口过滤特征列。
    window = "全量" → 全部特征
    window = "D3"   → 去掉 d7_ 前缀特征（只用前3天数据预测）
    window = "D1"   → 只保留 d1_ 特征 + 非时间窗口特征
    """
    if window == "全量":
        return feat_cols
    elif window == "D3":
        # 去掉 d7_ 前缀的特征，模拟只有前3天数据时的预测能力
        return [c for c in feat_cols if not c.startswith(D7_PREFIX)]
    elif window == "D1":
        # 只保留 d1_ 前缀特征 + 非 d3_/d7_ 前缀的特征
        return [c for c in feat_cols
                if not c.startswith(D3_PREFIX) and not c.startswith(D7_PREFIX)]
    return feat_cols


# ══════════════════════════════════════════════════════════════
# 1. 数据加载
# ══════════════════════════════════════════════════════════════
def load_data():
    print("\n[DATA] 加载特征表 ...")
    train_sdf = spark.read.parquet(f"{FEATURE_DIR}/features_train.parquet")
    dev_sdf   = spark.read.parquet(f"{FEATURE_DIR}/features_dev.parquet")

    # 确定特征列（取 Spark schema 中的数值类型列）
    numeric_types = {"double", "float", "integer", "long"}
    all_feat = [c for c in train_sdf.columns
                if c not in NON_FEAT
                and train_sdf.schema[c].dataType.typeName() in numeric_types]

    # 用 EDA 白名单过滤
    kept = load_kept_features()
    feat_cols = [c for c in all_feat if kept is None or c in kept]

    # 计算类别不平衡权重
    pos = train_sdf.filter(F.col("label") == 1).count()
    neg = train_sdf.filter(F.col("label") == 0).count()
    scale_pos_weight = neg / pos   # 用于 LightGBM / XGBoost
    print(f"  训练集: {pos+neg:,} 用户  正负比 1:{scale_pos_weight:.2f}")
    print(f"  验证集: {dev_sdf.count():,} 用户")
    print(f"  入模特征数: {len(feat_cols)}")

    return train_sdf, dev_sdf, feat_cols, scale_pos_weight


# ══════════════════════════════════════════════════════════════
# 2. 构建 Spark MLlib Pipeline
# ══════════════════════════════════════════════════════════════
def build_spark_pipeline(train_sdf, feat_cols):
    """构建 Imputer → Assembler → Scaler 流水线"""
    imp_out = [f"{c}_imp" for c in feat_cols]

    imputer   = Imputer(inputCols=feat_cols, outputCols=imp_out, strategy="mean")
    assembler = VectorAssembler(inputCols=imp_out, outputCol="features_raw",
                                handleInvalid="keep")
    scaler    = StandardScaler(inputCol="features_raw", outputCol="features",
                               withMean=True, withStd=True)

    pipeline       = Pipeline(stages=[imputer, assembler, scaler])
    pipeline_model = pipeline.fit(train_sdf)
    return pipeline_model


# ══════════════════════════════════════════════════════════════
# 3. 为不平衡样本生成 sampleWeight 列（RF/GBT 用）
# ══════════════════════════════════════════════════════════════
def add_sample_weight(sdf, scale_pos_weight):
    """
    给正样本赋予更高权重，补偿类别不平衡。
    Spark RF/GBT 支持 weightCol 参数。
    """
    return sdf.withColumn(
        "sample_weight",
        F.when(F.col("label") == 1, scale_pos_weight).otherwise(1.0)
    )


# ══════════════════════════════════════════════════════════════
# 4. 随机森林（Spark MLlib + 交叉验证）
# ══════════════════════════════════════════════════════════════
def train_random_forest(train_proc, dev_proc, feat_cols, scale_pos_weight, window="全量"):
    print(f"\n[RF] 随机森林 [{window}] ...")
    t0 = time.time()

    rf = RandomForestClassifier(
        labelCol="label", featuresCol="features",
        weightCol="sample_weight",
        seed=RANDOM_SEED
    )

    param_grid = ParamGridBuilder() \
        .addGrid(rf.numTrees,              [100, 200]) \
        .addGrid(rf.maxDepth,              [5, 8]) \
        .addGrid(rf.minInstancesPerNode,   [5, 10]) \
        .build()

    evaluator = BinaryClassificationEvaluator(
        labelCol="label", metricName="areaUnderROC"
    )
    cv = CrossValidator(
        estimator=rf, estimatorParamMaps=param_grid,
        evaluator=evaluator, numFolds=CV_FOLDS,
        seed=RANDOM_SEED, parallelism=2
    )
    best = cv.fit(train_proc).bestModel
    print(f"  最优参数: numTrees={best.getNumTrees}  maxDepth={best.getMaxDepth()}")

    pred = best.transform(dev_proc)
    pdf  = pred.select("label","prediction","probability").toPandas()
    y_true = pdf["label"].astype(int)
    y_pred = pdf["prediction"].astype(int)
    y_prob = pdf["probability"].apply(lambda x: float(x[1]))
    metrics = calc_metrics("RandomForest", y_true, y_pred, y_prob, window)
    metrics["train_time_s"] = round(time.time() - t0, 1)

    # 特征重要性
    imp = sorted(zip(feat_cols, best.featureImportances.toArray()),
                 key=lambda x: -x[1])[:20]
    metrics["feature_importance"] = imp
    print("  Top5 特征:", [f[0] for f in imp[:5]])

    # 保存
    if window == "全量":
        best.write().overwrite().save(f"{MODEL_DIR}/rf_model")
    return metrics, y_true, y_prob


# ══════════════════════════════════════════════════════════════
# 5. 梯度提升树（Spark MLlib）
# ══════════════════════════════════════════════════════════════
def train_gbt(train_proc, dev_proc, feat_cols, scale_pos_weight, window="全量"):
    print(f"\n[GBT] 梯度提升树 [{window}] ...")
    t0 = time.time()

    gbt = GBTClassifier(
        labelCol="label", featuresCol="features",
        weightCol="sample_weight",
        seed=RANDOM_SEED
    )

    # GBT 超参网格（缩小规模，避免在虚拟机上超时）
    param_grid = ParamGridBuilder() \
        .addGrid(gbt.maxDepth,  [4, 6]) \
        .addGrid(gbt.maxIter,   [50, 100]) \
        .addGrid(gbt.stepSize,  [0.1, 0.05]) \
        .build()

    evaluator = BinaryClassificationEvaluator(
        labelCol="label", metricName="areaUnderROC"
    )
    cv = CrossValidator(
        estimator=gbt, estimatorParamMaps=param_grid,
        evaluator=evaluator, numFolds=CV_FOLDS,
        seed=RANDOM_SEED, parallelism=2
    )
    best = cv.fit(train_proc).bestModel

    pred = best.transform(dev_proc)
    # GBT 的 rawPrediction 第二列是正类分数，转化为概率
    pred = pred.withColumn(
        "probability_pos",
        F.udf(lambda v: float(1 / (1 + float(np.exp(-v[1])))), "double")(
            F.col("rawPrediction")
        )
    )
    pdf    = pred.select("label","prediction","probability_pos").toPandas()
    y_true = pdf["label"].astype(int)
    y_pred = pdf["prediction"].astype(int)
    y_prob = pdf["probability_pos"].astype(float)
    metrics = calc_metrics("GBT", y_true, y_pred, y_prob, window)
    metrics["train_time_s"] = round(time.time() - t0, 1)

    if window == "全量":
        best.write().overwrite().save(f"{MODEL_DIR}/gbt_model")
    return metrics, y_true, y_prob


# ══════════════════════════════════════════════════════════════
# 6. LightGBM（pandas driver，StratifiedKFold 交叉验证）
# ══════════════════════════════════════════════════════════════
def train_lightgbm(X_train, y_train, X_dev, y_dev, feat_cols,
                   scale_pos_weight, window="全量"):
    print(f"\n[LGBM] LightGBM [{window}] ...")
    t0 = time.time()

    params = {
        "objective"        : "binary",
        "metric"           : "auc",
        "learning_rate"    : 0.05,
        "num_leaves"       : 63,
        "max_depth"        : -1,
        "min_child_samples": 20,
        "feature_fraction" : 0.8,
        "bagging_fraction" : 0.8,
        "bagging_freq"     : 5,
        "lambda_l1"        : 0.1,
        "lambda_l2"        : 0.1,
        "scale_pos_weight" : scale_pos_weight,   # 类别不平衡补偿
        "seed"             : RANDOM_SEED,
        "verbose"          : -1,
    }

    # ── StratifiedKFold 交叉验证（在训练集上）──
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

    print(f"  CV AUC: {np.mean(cv_aucs):.4f} ± {np.std(cv_aucs):.4f}")

    # ── 用全量训练集训练最终模型 ──
    dtrain = lgb.Dataset(X_train, label=y_train,
                         feature_name=list(feat_cols))
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
    metrics["cv_auc_mean"] = round(np.mean(cv_aucs), 4)
    metrics["cv_auc_std"]  = round(np.std(cv_aucs), 4)
    metrics["best_iteration"] = final_model.best_iteration
    metrics["train_time_s"]   = round(time.time() - t0, 1)

    # 特征重要性
    imp_df = pd.DataFrame({
        "feature"   : final_model.feature_name(),
        "importance": final_model.feature_importance(importance_type="gain")
    }).sort_values("importance", ascending=False)
    metrics["feature_importance"] = imp_df.values.tolist()[:20]
    print(f"  Top5 特征: {imp_df['feature'].head(5).tolist()}")
    print(f"  最优迭代数: {final_model.best_iteration}  训练耗时: {metrics['train_time_s']}s")

    if window == "全量":
        final_model.save_model(f"{MODEL_DIR}/lightgbm_full.txt")

    return metrics, y_dev, y_prob


# ══════════════════════════════════════════════════════════════
# 7. XGBoost（pandas driver）
# ══════════════════════════════════════════════════════════════
def train_xgboost(X_train, y_train, X_dev, y_dev, feat_cols,
                  scale_pos_weight, window="全量"):
    print(f"\n[XGB] XGBoost [{window}] ...")
    t0 = time.time()

    params = {
        "objective"        : "binary:logistic",
        "eval_metric"      : "auc",
        "learning_rate"    : 0.05,
        "max_depth"        : 6,
        "min_child_weight" : 5,
        "subsample"        : 0.8,
        "colsample_bytree" : 0.8,
        "reg_alpha"        : 0.1,
        "reg_lambda"       : 1.0,
        "scale_pos_weight" : scale_pos_weight,   # 类别不平衡补偿
        "seed"             : RANDOM_SEED,
        "verbosity"        : 0,
    }

    # StratifiedKFold CV
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

    print(f"  CV AUC: {np.mean(cv_aucs):.4f} ± {np.std(cv_aucs):.4f}")

    # 最终模型
    dtrain = xgb.DMatrix(X_train, label=y_train)
    ddev   = xgb.DMatrix(X_dev,   label=y_dev)
    final_model = xgb.train(
        params, dtrain, num_boost_round=1000,
        evals=[(ddev, "dev")], early_stopping_rounds=50,
        verbose_eval=100
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
                           columns=["feature","importance"]) \
               .sort_values("importance", ascending=False)
    metrics["feature_importance"] = imp_df.values.tolist()[:20]

    if window == "全量":
        final_model.save_model(f"{MODEL_DIR}/xgboost_full.json")

    return metrics, y_dev, y_prob


# ══════════════════════════════════════════════════════════════
# 8. 主流程
# ══════════════════════════════════════════════════════════════
def main():
    # ─── 加载数据 ───
    train_sdf, dev_sdf, feat_cols, spw = load_data()

    # ─── 为 pandas 模型准备数据（只做一次 collect）───
    def to_pandas(sdf, cols):
        pdf = sdf.select(["label"] + cols).toPandas()
        X   = pdf[cols].fillna(pdf[cols].median())
        y   = pdf["label"].astype(int)
        return X, y

    print("\n[COLLECT] 转换为 pandas ...")
    X_train_all, y_train = to_pandas(train_sdf, feat_cols)
    X_dev_all,   y_dev   = to_pandas(dev_sdf,   feat_cols)

    # ─── 定义三组时间窗口实验 ───
    windows = {
        "全量": feat_cols,
        "D3"  : get_window_features(feat_cols, "D3"),
        "D1"  : get_window_features(feat_cols, "D1"),
    }
    print("\n[EXPERIMENT] 时间窗口实验设计:")
    for w, cols in windows.items():
        print(f"  {w}: {len(cols)} 个特征")

    roc_data = {}   # 保存 ROC 曲线数据

    # ─── 对每个时间窗口跑 LightGBM + XGBoost ───
    for window, wcols in windows.items():
        X_tr = X_train_all[wcols]
        X_dv = X_dev_all[wcols]

        lgbm_m, yt, yp = train_lightgbm(X_tr, y_train, X_dv, y_dev,
                                          wcols, spw, window)
        all_results.append(lgbm_m)
        if window == "全量":
            roc_data["LightGBM"] = (yt, yp)

        xgb_m, yt, yp = train_xgboost(X_tr, y_train, X_dv, y_dev,
                                        wcols, spw, window)
        all_results.append(xgb_m)
        if window == "全量":
            roc_data["XGBoost"] = (yt, yp)

    # ─── RF / GBT 只做全量实验（Spark 分布式，耗时较长）───
    print("\n[SPARK] 构建 Spark Pipeline（全量特征）...")
    train_weighted = add_sample_weight(train_sdf, spw)
    dev_weighted   = add_sample_weight(dev_sdf,   spw)
    pipeline_model = build_spark_pipeline(train_weighted, feat_cols)
    
    # 保存特征预处理 Pipeline（推理必须用同一套）
    PIPELINE_DIR = f"{MODEL_DIR}/spark_feature_pipeline"
    pipeline_model.write().overwrite().save(PIPELINE_DIR)
    print(f"[SAVE] pipeline saved to: {PIPELINE_DIR}")

    train_proc = pipeline_model.transform(train_weighted)
    dev_proc   = pipeline_model.transform(dev_weighted)

    rf_m, yt, yp = train_random_forest(train_proc, dev_proc, feat_cols, spw)
    all_results.append(rf_m)
    roc_data["RandomForest"] = (yt, yp)

    gbt_m, yt, yp = train_gbt(train_proc, dev_proc, feat_cols, spw)
    all_results.append(gbt_m)
    roc_data["GBT"] = (yt, yp)

    # ─── 汇总输出 ───
    summarize_results(roc_data)

    spark.stop()
    print("\n 模型训练完成")


# ══════════════════════════════════════════════════════════════
# 9. 汇总报告
# ══════════════════════════════════════════════════════════════
def summarize_results(roc_data):
    print("\n" + "=" * 70)
    print("模型对比汇总（验证集）")
    print("=" * 70)

    summary_cols = ["model", "window", "auc", "f1", "accuracy",
                    "precision", "recall", "cv_auc_mean", "cv_auc_std",
                    "best_iteration", "train_time_s", "tn", "fp", "fn", "tp"]
    rows = []
    for m in all_results:
        row = {k: m.get(k, "") for k in summary_cols}
        rows.append(row)

    df = pd.DataFrame(rows).sort_values(["window", "auc"], ascending=[True, False])
    print(df.to_string(index=False))
    df.to_csv(f"{RESULT_DIR}/model_comparison_full.csv", index=False)

    # 保存特征重要性
    for m in all_results:
        if "feature_importance" in m and m["window"] == "全量":
            fi = m["feature_importance"]
            fi_df = pd.DataFrame(fi, columns=["feature", "importance"])
            fname = f"feat_imp_{m['model'].lower()}.csv"
            fi_df.to_csv(f"{RESULT_DIR}/{fname}", index=False)

    # 全量模型中最优者
    full_df = df[df["window"] == "全量"].sort_values("auc", ascending=False)
    best    = full_df.iloc[0]
    print(f"\n 最优全量模型: {best['model']}  "
          f"AUC={best['auc']:.4f}  F1={best['f1']:.4f}")

    # 时间窗口实验摘要
    print("\n 时间窗口实验（LightGBM）:")
    lgbm_df = df[df["model"] == "LightGBM"][["window", "auc", "f1"]]
    print(lgbm_df.to_string(index=False))

    # 保存 ROC 曲线原始数据（供绘图脚本使用）
    roc_export = {}
    for model_name, (yt, yp) in roc_data.items():
        fpr, tpr, _ = roc_curve(np.array(yt), np.array(yp))
        roc_export[model_name] = {
            "fpr": fpr.tolist(),
            "tpr": tpr.tolist(),
            "auc": round(auc(fpr, tpr), 4)
        }
    with open(f"{RESULT_DIR}/roc_data.json", "w") as f:
        json.dump(roc_export, f)
    print(f"\n  ROC 曲线数据已保存: {RESULT_DIR}/roc_data.json")

    # 保存最优模型名称
    with open(f"{RESULT_DIR}/best_model.txt", "w") as f:
        f.write(best["model"])

    print(f"\n  所有结果保存至: {RESULT_DIR}/")


if __name__ == "__main__":
    main()
