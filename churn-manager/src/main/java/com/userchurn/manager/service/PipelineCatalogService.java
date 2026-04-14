package com.userchurn.manager.service;

import com.userchurn.manager.domain.ManagedProject;
import com.userchurn.manager.domain.PipelineStep;
import com.userchurn.manager.domain.PipelineTemplate;
import com.userchurn.manager.repo.PipelineTemplateRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
public class PipelineCatalogService {

    public record StepDefinition(String key, String displayName, String description, String commandTemplate) {
    }

    private final PipelineTemplateRepository pipelineTemplateRepository;
    private final Map<String, StepDefinition> catalog;

    public PipelineCatalogService(PipelineTemplateRepository pipelineTemplateRepository) {
        this.pipelineTemplateRepository = pipelineTemplateRepository;
        this.catalog = buildCatalog();
    }

    public List<StepDefinition> listCatalog() {
        return new ArrayList<>(catalog.values());
    }

    public StepDefinition getRequiredStep(String stepKey) {
        StepDefinition definition = catalog.get(stepKey);
        if (definition == null) {
            throw new IllegalArgumentException("Unknown step key: " + stepKey);
        }
        return definition;
    }

    public String renderCommand(ManagedProject project, StepDefinition definition) {
        Map<String, String> placeholders = Map.of(
            "{projectRoot}", safe(project.getProjectRoot()),
            "{scriptsDir}", safe(project.getScriptsDir()),
            "{pythonCommand}", safe(project.getPythonCommand()),
            "{sparkSubmitCommand}", safe(project.getSparkSubmitCommand()),
            "{beelineCommand}", safe(project.getBeelineCommand())
        );
        String rendered = definition.commandTemplate();
        for (Map.Entry<String, String> entry : placeholders.entrySet()) {
            rendered = rendered.replace(entry.getKey(), entry.getValue());
        }
        return rendered;
    }

    @Transactional
    public PipelineTemplate ensureDefaultPipeline(ManagedProject project) {
        return pipelineTemplateRepository.findByProjectIdOrderByUpdatedAtDesc(project.getId()).stream()
            .filter(template -> Boolean.TRUE.equals(template.getBuiltIn()) && Objects.equals(template.getName(), "标准全链路"))
            .findFirst()
            .orElseGet(() -> createPipeline(project, "标准全链路", "按当前数据中心链路预置的完整执行流程。", defaultStepKeys(), true));
    }

    @Transactional
    public PipelineTemplate createPipeline(ManagedProject project,
                                           String name,
                                           String description,
                                           List<String> stepKeys,
                                           boolean builtIn) {
        PipelineTemplate pipeline = new PipelineTemplate();
        pipeline.setProject(project);
        pipeline.setName(name);
        pipeline.setDescription(description);
        pipeline.setBuiltIn(builtIn);

        List<PipelineStep> steps = new ArrayList<>();
        for (int i = 0; i < stepKeys.size(); i++) {
            StepDefinition definition = getRequiredStep(stepKeys.get(i));
            PipelineStep step = new PipelineStep();
            step.setStepOrder(i + 1);
            step.setStepKey(definition.key());
            step.setDisplayName(definition.displayName());
            step.setCommandTemplate(definition.commandTemplate());
            steps.add(step);
        }
        pipeline.replaceSteps(steps);
        return pipelineTemplateRepository.save(pipeline);
    }

    public List<String> defaultStepKeys() {
        return List.of(
            "transform-data",
            "split-battle-log",
            "merge-user-labels",
            "load-ods",
            "etl-v3",
            "eda",
            "train-model",
            "plot-results",
            "rule-engine"
        );
    }

    private Map<String, StepDefinition> buildCatalog() {
        Map<String, StepDefinition> definitions = new LinkedHashMap<>();
        definitions.put("transform-data", new StepDefinition(
            "transform-data",
            "11 数据改造",
            "将 Origin_Data 映射为塔防业务风格的行为与地图数据。",
            "{pythonCommand} \"{scriptsDir}/11_data_transform.py\""
        ));
        definitions.put("split-battle-log", new StepDefinition(
            "split-battle-log",
            "12 战斗日志按日期切分",
            "将 battle_log 按 dt 切成多份，便于后续分区入仓。",
            "{pythonCommand} \"{scriptsDir}/12_split_battle_log_by_dt.py\""
        ));
        definitions.put("merge-user-labels", new StepDefinition(
            "merge-user-labels",
            "13 合并用户标签",
            "把 train/dev/test 合并为统一 user_label_all。",
            "{pythonCommand} \"{scriptsDir}/13_make_user_label_all.py\""
        ));
        definitions.put("load-ods", new StepDefinition(
            "load-ods",
            "14 ODS 入仓",
            "通过 spark-submit 将 CSV 上传到 HDFS 并写入 Hive ODS。",
            "{sparkSubmitCommand} \"{scriptsDir}/14_ods_loader.py\""
        ));
        definitions.put("etl-v3", new StepDefinition(
            "etl-v3",
            "20 ETL 特征构建",
            "执行 ODS -> DWD/DWS/ADS 特征主链路。",
            "{sparkSubmitCommand} \"{scriptsDir}/20_etl_pipeline_v3.py\""
        ));
        definitions.put("eda", new StepDefinition(
            "eda",
            "31 探索性分析",
            "输出特征空值率、相关性和分布分析结果。",
            "{pythonCommand} \"{scriptsDir}/31_eda.py\""
        ));
        definitions.put("train-model", new StepDefinition(
            "train-model",
            "32 模型训练",
            "训练并比较多种模型，输出指标与重要特征。",
            "{pythonCommand} \"{scriptsDir}/32_train.py\""
        ));
        definitions.put("plot-results", new StepDefinition(
            "plot-results",
            "33 结果绘图",
            "根据实验结果绘制 ROC、特征重要性和混淆矩阵等图表。",
            "{pythonCommand} \"{scriptsDir}/33_plot.py\""
        ));
        definitions.put("rule-engine", new StepDefinition(
            "rule-engine",
            "40 规则引擎预警",
            "结合模型与规则输出风险结果、每日汇总和 ADS 写出。",
            "{sparkSubmitCommand} \"{scriptsDir}/40_rule_engine_v4.py\""
        ));
        return definitions;
    }

    private String safe(String value) {
        return value == null ? "" : value;
    }
}
