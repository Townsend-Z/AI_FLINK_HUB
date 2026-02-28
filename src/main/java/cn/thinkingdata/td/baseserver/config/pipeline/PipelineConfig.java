package cn.thinkingdata.td.baseserver.config.pipeline;

import cn.thinkingdata.td.baseserver.config.JobConfig;
import cn.thinkingdata.td.baseserver.config.KafkaConfig;
import cn.thinkingdata.td.baseserver.config.PaimonConfig;
import cn.thinkingdata.td.baseserver.schema.SchemaDefinition;

import java.util.List;

/**
 * 从数据库加载后组装的完整 Pipeline 配置，包含：
 * Kafka、Paimon、Schema、处理器链定义。
 */
public class PipelineConfig {

    private String jobName;
    private int parallelism = 1;
    private KafkaConfig kafkaConfig;
    private PaimonConfig paimonConfig;
    private SchemaDefinition schema;
    private List<ProcessorSpec> processors;

    public PipelineConfig() {}

    public String getJobName()               { return jobName; }
    public void setJobName(String v)         { this.jobName = v; }
    public int getParallelism()              { return parallelism; }
    public void setParallelism(int v)        { this.parallelism = v; }
    public KafkaConfig getKafkaConfig()      { return kafkaConfig; }
    public void setKafkaConfig(KafkaConfig v){ this.kafkaConfig = v; }
    public PaimonConfig getPaimonConfig()    { return paimonConfig; }
    public void setPaimonConfig(PaimonConfig v){ this.paimonConfig = v; }
    public SchemaDefinition getSchema()      { return schema; }
    public void setSchema(SchemaDefinition v){ this.schema = v; }
    public List<ProcessorSpec> getProcessors(){ return processors; }
    public void setProcessors(List<ProcessorSpec> v){ this.processors = v; }

    /** 转换为 JobConfig（供 FlinkPipeline 使用） */
    public JobConfig toJobConfig() {
        return JobConfig.builder()
                .jobName(jobName)
                .parallelism(parallelism)
                .kafkaConfig(kafkaConfig)
                .paimonConfig(paimonConfig)
                .schema(schema)
                .build();
    }
}
