"""
train.spark_models
==================
Spark MLlib models: RandomForest and GBT with CrossValidator,
plus feature Pipeline and sample weighting utilities.
"""

import time
import numpy as np

from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, Imputer
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

from config_env import env, env_int
from train.data_loader import calc_metrics

MODEL_DIR   = env("TD_CHURN_MODEL_DIR",
                   env("TD_CHURN_PROJECT_DIR", "/home/hadoop/td_churn_project")
                   + "/models")
RANDOM_SEED = env_int("TD_CHURN_RANDOM_SEED", 42)
CV_FOLDS    = env_int("TD_CHURN_CV_FOLDS", 5)


def build_spark_pipeline(train_sdf, feat_cols):
    """Build Imputer -> Assembler -> Scaler pipeline and fit on train data."""
    imp_out   = [f"{c}_imp" for c in feat_cols]
    imputer   = Imputer(inputCols=feat_cols, outputCols=imp_out, strategy="mean")
    assembler = VectorAssembler(inputCols=imp_out, outputCol="features_raw",
                                handleInvalid="keep")
    scaler    = StandardScaler(inputCol="features_raw", outputCol="features",
                               withMean=False, withStd=True)
    pipeline       = Pipeline(stages=[imputer, assembler, scaler])
    pipeline_model = pipeline.fit(train_sdf)
    return pipeline_model


def add_sample_weight(sdf, scale_pos_weight):
    """Add sample_weight column for class imbalance compensation."""
    return sdf.withColumn(
        "sample_weight",
        F.when(F.col("label") == 1, scale_pos_weight).otherwise(1.0)
    )


def train_random_forest(train_proc, dev_proc, feat_cols, scale_pos_weight,
                        window="full"):
    """Train RF with CrossValidator and return (metrics, y_true, y_prob)."""
    print(f"\n[RF] RandomForest [{window}] ...")
    t0 = time.time()

    rf = RandomForestClassifier(
        labelCol="label", featuresCol="features",
        weightCol="sample_weight", seed=RANDOM_SEED,
    )
    param_grid = ParamGridBuilder() \
        .addGrid(rf.numTrees,            [150]) \
        .addGrid(rf.maxDepth,            [5, 8]) \
        .addGrid(rf.minInstancesPerNode, [5, 10]) \
        .build()
    evaluator = BinaryClassificationEvaluator(
        labelCol="label", metricName="areaUnderROC")
    cv = CrossValidator(
        estimator=rf, estimatorParamMaps=param_grid,
        evaluator=evaluator, numFolds=CV_FOLDS,
        seed=RANDOM_SEED, parallelism=2,
    )
    best = cv.fit(train_proc).bestModel
    print(f"  Best params: numTrees={best.getNumTrees}  "
          f"maxDepth={best.getMaxDepth()}")

    pred = best.transform(dev_proc)
    pdf  = pred.select("label", "prediction", "probability").toPandas()
    y_true = pdf["label"].astype(int)
    y_pred = pdf["prediction"].astype(int)
    y_prob = pdf["probability"].apply(lambda x: float(x[1]))
    metrics = calc_metrics("RandomForest", y_true, y_pred, y_prob, window)
    metrics["train_time_s"] = round(time.time() - t0, 1)

    imp = sorted(zip(feat_cols, best.featureImportances.toArray()),
                 key=lambda x: -x[1])[:20]
    metrics["feature_importance"] = imp
    print("  Top5 features:", [f[0] for f in imp[:5]])

    if window == "full":
        best.write().overwrite().save(f"{MODEL_DIR}/rf_model")
    return metrics, y_true, y_prob


def train_gbt(train_proc, dev_proc, feat_cols, scale_pos_weight,
              window="full"):
    """Train GBT with CrossValidator and return (metrics, y_true, y_prob)."""
    print(f"\n[GBT] Gradient Boosted Trees [{window}] ...")
    t0 = time.time()

    gbt = GBTClassifier(
        labelCol="label", featuresCol="features",
        weightCol="sample_weight", seed=RANDOM_SEED,
    )
    param_grid = ParamGridBuilder() \
        .addGrid(gbt.maxDepth, [4, 6]) \
        .addGrid(gbt.maxIter,  [80]) \
        .addGrid(gbt.stepSize, [0.1, 0.05]) \
        .build()
    evaluator = BinaryClassificationEvaluator(
        labelCol="label", metricName="areaUnderROC")
    cv = CrossValidator(
        estimator=gbt, estimatorParamMaps=param_grid,
        evaluator=evaluator, numFolds=CV_FOLDS,
        seed=RANDOM_SEED, parallelism=2,
    )
    best = cv.fit(train_proc).bestModel

    pred = best.transform(dev_proc)
    pred = pred.withColumn(
        "probability_pos",
        F.udf(lambda v: float(1 / (1 + float(np.exp(-v[1])))), "double")(
            F.col("rawPrediction")
        )
    )
    pdf    = pred.select("label", "prediction", "probability_pos").toPandas()
    y_true = pdf["label"].astype(int)
    y_pred = pdf["prediction"].astype(int)
    y_prob = pdf["probability_pos"].astype(float)
    metrics = calc_metrics("GBT", y_true, y_pred, y_prob, window)
    metrics["train_time_s"] = round(time.time() - t0, 1)

    if window == "full":
        best.write().overwrite().save(f"{MODEL_DIR}/gbt_model")
    return metrics, y_true, y_prob
