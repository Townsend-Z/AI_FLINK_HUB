package cn.thinkingdata.td.baseserver.pipeline;

import cn.thinkingdata.td.baseserver.ai.factory.AiFunctionFactory;
import cn.thinkingdata.td.baseserver.config.pipeline.PipelineConfig;
import cn.thinkingdata.td.baseserver.config.pipeline.ProcessorSpec;
import cn.thinkingdata.td.baseserver.operator.ConfigurableAggregateProcessor;
import cn.thinkingdata.td.baseserver.operator.ConfigurableFilterProcessor;
import cn.thinkingdata.td.baseserver.operator.ConfigurableTransformProcessor;
import cn.thinkingdata.td.baseserver.operator.rule.AggregateRule;
import cn.thinkingdata.td.baseserver.operator.rule.FilterRule;
import cn.thinkingdata.td.baseserver.operator.rule.TransformRule;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import cn.thinkingdata.td.baseserver.processor.DataProcessor;
import cn.thinkingdata.td.baseserver.config.JobConfig;
import cn.thinkingdata.td.baseserver.config.KafkaConfig;
import cn.thinkingdata.td.baseserver.config.PaimonConfig;
import cn.thinkingdata.td.baseserver.schema.SchemaDefinition;
import cn.thinkingdata.td.baseserver.schema.SchemaField;
import cn.thinkingdata.td.baseserver.serialization.SchemaBasedDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Flink Pipeline 核心组装器。
 *
 * <pre>
 * 执行流程：
 *   Kafka Source
 *     → SchemaBasedDeserializer（JSON → Row）
 *     → DataProcessor 链（Transform / Filter / Aggregate）
 *     → Table API 桥接
 *     → Paimon Sink
 * </pre>
 *
 * 使用示例：
 * <pre>{@code
 * FlinkPipeline.builder(jobConfig)
 *     .addProcessor(new FilterProcessor())
 *     .addProcessor(new TransformProcessor())
 *     .build()
 *     .execute();
 * }</pre>
 */
public class FlinkPipeline {

    private final JobConfig jobConfig;
    private final List<DataProcessor> processors;

    private FlinkPipeline(Builder builder) {
        this.jobConfig  = builder.jobConfig;
        this.processors = builder.processors;
    }

    // -------------------------------------------------------------------------
    //  Public API
    // -------------------------------------------------------------------------

    public static Builder builder(JobConfig config) {
        return new Builder(config);
    }

    /**
     * 从 {@link PipelineConfig}（数据库加载的配置）构建 FlinkPipeline。
     * 自动根据 processor type 创建对应的 DataProcessor 实例。
     */
    public static FlinkPipeline fromConfig(PipelineConfig config) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        SchemaDefinition schema = config.getSchema();
        Builder builder = FlinkPipeline.builder(config.toJobConfig());

        for (ProcessorSpec spec : config.getProcessors()) {
            switch (spec.getType()) {
                case "filter":
                    FilterRule filterRule = mapper.readValue(spec.getProcessorConfig(), FilterRule.class);
                    builder.addProcessor(new ConfigurableFilterProcessor(filterRule, schema));
                    break;
                case "transform":
                    TransformRule transformRule = mapper.readValue(spec.getProcessorConfig(), TransformRule.class);
                    builder.addProcessor(new ConfigurableTransformProcessor(transformRule, schema));
                    break;
                case "aggregate":
                    AggregateRule aggregateRule = mapper.readValue(spec.getProcessorConfig(), AggregateRule.class);
                    builder.addProcessor(new ConfigurableAggregateProcessor(aggregateRule, schema));
                    break;
                case "ai-inference":
                    builder.addProcessor(AiFunctionFactory.create(spec.getAiFunctionSpec()));
                    break;
                default:
                    throw new IllegalArgumentException("未知 processor type: " + spec.getType());
            }
        }
        return builder.build();
    }

    /**
     * 组装并启动 Flink 流式作业。
     *
     * @return TableResult（可用于等待作业结束或获取 JobClient）
     */
    public TableResult execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(jobConfig.getParallelism());

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 注册 Paimon Catalog
        setupPaimonCatalog(tableEnv);

        // 2. 按 SchemaDefinition 自动建表（IF NOT EXISTS）
        createTableIfNotExists(tableEnv);

        // 3. 构建 Kafka Source
        DataStream<Row> sourceStream = buildKafkaSource(env);

        // 4. 依次执行处理器链
        DataStream<Row> processedStream = sourceStream;
        for (DataProcessor processor : processors) {
            processedStream = processor.process(processedStream);
        }

        // 5. DataStream → Table，写入 Paimon
        // 通过 identity map + returns() 重新声明 RowTypeInfo（确保 map/transform 后字段名不丢失）
        List<SchemaField> fields = jobConfig.getSchema().getFields();
        String[] names = fields.stream().map(SchemaField::getName).toArray(String[]::new);
        TypeInformation<?>[] types = fields.stream().map(this::toFlinkTypeInfo).toArray(TypeInformation[]::new);
        TypeInformation<Row> rowType = new RowTypeInfo(types, names);
        DataStream<Row> typedStream = processedStream.map(row -> row).returns(rowType);
        Table inputTable = tableEnv.fromDataStream(typedStream, buildFlinkSchema());
        String targetTable = buildTargetTableIdentifier();
        return inputTable.executeInsert(targetTable);
    }

    // -------------------------------------------------------------------------
    //  Private helpers
    // -------------------------------------------------------------------------

    private void setupPaimonCatalog(StreamTableEnvironment tableEnv) {
        PaimonConfig paimon = jobConfig.getPaimonConfig();
        tableEnv.executeSql(String.format(
                "CREATE CATALOG `%s` WITH ('type'='paimon', 'warehouse'='%s')",
                paimon.getCatalogName(), paimon.getWarehouse()));
        tableEnv.executeSql("USE CATALOG `" + paimon.getCatalogName() + "`");
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS `" + paimon.getDatabase() + "`");
        tableEnv.executeSql("USE `" + paimon.getDatabase() + "`");
    }

    private void createTableIfNotExists(StreamTableEnvironment tableEnv) {
        SchemaDefinition schema = jobConfig.getSchema();
        PaimonConfig paimon     = jobConfig.getPaimonConfig();

        StringBuilder sql = new StringBuilder();
        sql.append(String.format("CREATE TABLE IF NOT EXISTS `%s`.`%s`.`%s` (\n",
                paimon.getCatalogName(), paimon.getDatabase(), schema.getTableName()));

        List<SchemaField> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            SchemaField f = fields.get(i);
            sql.append(String.format("  `%s` %s", f.getName(), toSqlType(f)));
            if (!f.isNullable()) sql.append(" NOT NULL");
            if (i < fields.size() - 1) sql.append(",\n");
        }

        if (!schema.getPrimaryKeys().isEmpty()) {
            sql.append(",\n  PRIMARY KEY (");
            sql.append(String.join(", ", schema.getPrimaryKeys()));
            sql.append(") NOT ENFORCED");
        }
        sql.append("\n)");

        if (!schema.getPartitionKeys().isEmpty()) {
            sql.append("\nPARTITIONED BY (");
            sql.append(String.join(", ", schema.getPartitionKeys()));
            sql.append(")");
        }

        // 合并 Paimon 表属性
        Map<String, String> props = paimon.getTableProperties();
        if (!props.isEmpty()) {
            sql.append("\nWITH (\n");
            StringJoiner sj = new StringJoiner(",\n");
            props.forEach((k, v) -> sj.add(String.format("  '%s' = '%s'", k, v)));
            sql.append(sj).append("\n)");
        }

        tableEnv.executeSql(sql.toString());
    }

    private DataStream<Row> buildKafkaSource(StreamExecutionEnvironment env) {
        KafkaConfig kafka           = jobConfig.getKafkaConfig();
        SchemaDefinition schema     = jobConfig.getSchema();
        SchemaBasedDeserializer des = new SchemaBasedDeserializer(schema);

        OffsetsInitializer offsetsInitializer;
        switch (kafka.getStartingOffset()) {
            case "earliest": offsetsInitializer = OffsetsInitializer.earliest(); break;
            case "timestamp": offsetsInitializer = OffsetsInitializer.timestamp(kafka.getStartingTimestamp()); break;
            default:         offsetsInitializer = OffsetsInitializer.latest();
        }

        KafkaSource<Row> kafkaSource = KafkaSource.<Row>builder()
                .setBootstrapServers(kafka.getBootstrapServers())
                .setTopics(kafka.getTopic())
                .setGroupId(kafka.getGroupId())
                .setStartingOffsets(offsetsInitializer)
                .setValueOnlyDeserializer(des)
                .build();

        String[] fieldNames = schema.getFields().stream()
                .map(SchemaField::getName).toArray(String[]::new);
        TypeInformation<?>[] fieldTypes = schema.getFields().stream()
                .map(this::toFlinkTypeInfo).toArray(TypeInformation[]::new);
        TypeInformation<Row> rowType = new RowTypeInfo(fieldTypes, fieldNames);

        return env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
                .returns(rowType);
    }

    private Schema buildFlinkSchema() {
        Schema.Builder builder = Schema.newBuilder();
        List<String> pks = jobConfig.getSchema().getPrimaryKeys();
        for (SchemaField field : jobConfig.getSchema().getFields()) {
            DataType dt = toFlinkDataType(field);
            // 主键字段或显式定义 not-null 的字段，标记为 NOT NULL
            if (!field.isNullable() || pks.contains(field.getName())) {
                dt = dt.notNull();
            }
            builder.column(field.getName(), dt);
        }
        if (!pks.isEmpty()) {
            builder.primaryKey(pks.toArray(new String[0]));
        }
        return builder.build();
    }

    private String buildTargetTableIdentifier() {
        PaimonConfig paimon    = jobConfig.getPaimonConfig();
        SchemaDefinition schema = jobConfig.getSchema();
        return String.format("`%s`.`%s`.`%s`",
                paimon.getCatalogName(), paimon.getDatabase(), schema.getTableName());
    }

    private String toSqlType(SchemaField f) {
        switch (f.getType()) {
            case STRING:    return "STRING";
            case INT:       return "INT";
            case LONG:      return "BIGINT";
            case FLOAT:     return "FLOAT";
            case DOUBLE:    return "DOUBLE";
            case BOOLEAN:   return "BOOLEAN";
            case DATE:      return "DATE";
            case TIMESTAMP: return "TIMESTAMP(3)";
            case DECIMAL:   return String.format("DECIMAL(%d, %d)", f.getPrecision(), f.getScale());
            default:        return "STRING";
        }
    }

    private DataType toFlinkDataType(SchemaField f) {
        switch (f.getType()) {
            case STRING:    return DataTypes.STRING();
            case INT:       return DataTypes.INT();
            case LONG:      return DataTypes.BIGINT();
            case FLOAT:     return DataTypes.FLOAT();
            case DOUBLE:    return DataTypes.DOUBLE();
            case BOOLEAN:   return DataTypes.BOOLEAN();
            case DATE:      return DataTypes.DATE();
            case TIMESTAMP: return DataTypes.TIMESTAMP(3);
            case DECIMAL:   return DataTypes.DECIMAL(f.getPrecision(), f.getScale());
            default:        return DataTypes.STRING();
        }
    }

    private TypeInformation<?> toFlinkTypeInfo(SchemaField f) {
        switch (f.getType()) {
            case STRING:    return Types.STRING;
            case INT:       return Types.INT;
            case LONG:      return Types.LONG;
            case FLOAT:     return Types.FLOAT;
            case DOUBLE:    return Types.DOUBLE;
            case BOOLEAN:   return Types.BOOLEAN;
            case DATE:      return Types.SQL_DATE;
            case TIMESTAMP: return Types.SQL_TIMESTAMP;
            case DECIMAL:   return Types.BIG_DEC;
            default:        return Types.STRING;
        }
    }

    // -------------------------------------------------------------------------
    //  Builder
    // -------------------------------------------------------------------------

    public static class Builder {
        private final JobConfig jobConfig;
        private final List<DataProcessor> processors = new ArrayList<>();

        private Builder(JobConfig jobConfig) {
            this.jobConfig = jobConfig;
        }

        public Builder addProcessor(DataProcessor processor) {
            this.processors.add(processor);
            return this;
        }

        public Builder addProcessors(List<DataProcessor> processors) {
            this.processors.addAll(processors);
            return this;
        }

        public FlinkPipeline build() {
            return new FlinkPipeline(this);
        }
    }
}
