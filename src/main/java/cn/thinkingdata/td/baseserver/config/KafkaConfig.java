package cn.thinkingdata.td.baseserver.config;

/**
 * Kafka Source 配置
 */
public class KafkaConfig {

    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    /** "earliest" | "latest" | "timestamp" */
    private final String startingOffset;
    /** startingOffset = "timestamp" 时使用 */
    private final long startingTimestamp;

    private KafkaConfig(Builder builder) {
        this.bootstrapServers = builder.bootstrapServers;
        this.topic = builder.topic;
        this.groupId = builder.groupId;
        this.startingOffset = builder.startingOffset;
        this.startingTimestamp = builder.startingTimestamp;
    }

    public String getBootstrapServers() { return bootstrapServers; }
    public String getTopic()            { return topic; }
    public String getGroupId()          { return groupId; }
    public String getStartingOffset()   { return startingOffset; }
    public long getStartingTimestamp()  { return startingTimestamp; }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private String bootstrapServers;
        private String topic;
        private String groupId  = "flink-paimon-group";
        private String startingOffset = "latest";
        private long startingTimestamp = -1L;

        public Builder bootstrapServers(String v) { this.bootstrapServers = v; return this; }
        public Builder topic(String v)            { this.topic = v; return this; }
        public Builder groupId(String v)          { this.groupId = v; return this; }
        public Builder startFromEarliest()        { this.startingOffset = "earliest"; return this; }
        public Builder startFromLatest()          { this.startingOffset = "latest"; return this; }
        public Builder startFromTimestamp(long ts){ this.startingOffset = "timestamp"; this.startingTimestamp = ts; return this; }

        public KafkaConfig build() { return new KafkaConfig(this); }
    }
}
