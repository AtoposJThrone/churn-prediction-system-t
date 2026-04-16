package com.churn.manager.execution;

import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Static catalog of built-in pipeline steps, matching the scripts in the data-processing center.
 * The command template uses {pythonCmd} and {sparkCmd} as placeholders resolved at runtime.
 */
@Component
public class StepCatalog {

    public record StepDef(
            String key,
            String displayName,
            String description,
            String executor,       // "python" | "spark"
            String scriptFile      // relative to scriptsDir
    ) {}

    private static final List<StepDef> STEPS = List.of(
            new StepDef("s11", "原始数据塔防化映射", "将原始 CSV 字段重命名为塔防语义，计算衍生特征",
                    "python", "11_data_transform.py"),
            new StepDef("s12", "战斗日志按日期切分", "按 battle_start_time 分区生成 battle_log_yyyy-MM-dd.csv",
                    "python", "12_split_battle_log_by_dt.py"),
            new StepDef("s13", "用户标签合并", "合并 train/dev/test，生成 user_label_all.csv",
                    "python", "13_make_user_label_all.py"),
            new StepDef("s14", "ODS 数据入仓", "CSV → HDFS → Hive ODS 分层建表与数据入仓",
                    "spark", "14_ods_loader.py"),
            new StepDef("s20", "ETL 全链路特征工程", "ODS → DWD → DWS → ADS，生成用户行为特征宽表",
                    "spark", "20_etl_pipeline_v3.py"),
            new StepDef("s31", "EDA 探索分析", "特征缺失率、相关性、流失 vs 留存分布，输出 EDA 报告",
                    "python", "31_eda.py"),
            new StepDef("s32", "模型训练对比", "RandomForest / GBT / LightGBM / XGBoost 五折交叉验证，序列化最优模型",
                    "spark", "32_train.py"),
            new StepDef("s33", "结果可视化出图", "ROC 曲线、特征重要性、混淆矩阵、模型对比柱状图",
                    "python", "33_plot.py"),
            new StepDef("s40", "规则引擎预警", "规则-模型混合推理，写出 alert_result.csv 和 MySQL ADS 层",
                    "spark", "40_rule_engine_v4.py")
    );

    public List<StepDef> all() {
        return STEPS;
    }

    public StepDef getOrThrow(String key) {
        return STEPS.stream()
                .filter(s -> s.key().equals(key))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown step key: " + key));
    }
}
