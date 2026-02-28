package cn.thinkingdata.td.baseserver.config;

import cn.thinkingdata.td.baseserver.schema.SchemaDefinition;

/**
 * 整体 Job 配置，聚合 KafkaConfig、PaimonConfig 以及 SchemaDefinition
 */
public class JobConfig {

    private final String jobName;
    private final int parallelism;
    private final KafkaConfig kafkaConfig;
    private final PaimonConfig paimonConfig;
    private final SchemaDefinition schema;

    private JobConfig(Builder builder) {
        this.jobName       = builder.jobName;
        this.parallelism   = builder.parallelism;
        this.kafkaConfig   = builder.kafkaConfig;
        this.paimonConfig  = builder.paimonConfig;
        this.schema        = builder.schema;
    }

    public String getJobName()           { return jobName; }
    public int getParallelism()          { return parallelism; }
    public KafkaConfig getKafkaConfig()  { return kafkaConfig; }
    public PaimonConfig getPaimonConfig(){ return paimonConfig; }
    public SchemaDefinition getSchema()  { return schema; }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private String jobName    = "kafka-to-paimon";
        private int parallelism   = 1;
        private KafkaConfig kafkaConfig;
        private PaimonConfig paimonConfig;
        private SchemaDefinition schema;

        public Builder jobName(String v)           { this.jobName = v; return this; }
        public Builder parallelism(int v)          { this.parallelism = v; return this; }
        public Builder kafkaConfig(KafkaConfig v)  { this.kafkaConfig = v; return this; }
        public Builder paimonConfig(PaimonConfig v){ this.paimonConfig = v; return this; }
        public Builder schema(SchemaDefinition v)  { this.schema = v; return this; }

        public JobConfig build() { return new JobConfig(this); }
    }
}
