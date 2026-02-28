package cn.thinkingdata.td.baseserver.db;

import cn.thinkingdata.td.baseserver.config.KafkaConfig;
import cn.thinkingdata.td.baseserver.config.PaimonConfig;
import cn.thinkingdata.td.baseserver.config.pipeline.AiFunctionSpec;
import cn.thinkingdata.td.baseserver.config.pipeline.PipelineConfig;
import cn.thinkingdata.td.baseserver.config.pipeline.ProcessorSpec;
import cn.thinkingdata.td.baseserver.schema.FieldType;
import cn.thinkingdata.td.baseserver.schema.SchemaDefinition;
import cn.thinkingdata.td.baseserver.schema.SchemaField;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 从数据库加载 Pipeline 配置，在 Flink 任务启动时调用一次。
 *
 * <p>查询顺序：
 * <ol>
 *   <li>td_flink_job → Kafka / Paimon / Schema 基础配置</li>
 *   <li>td_pipeline_processor → 处理链（按 step_order）</li>
 *   <li>td_ai_function → AI 函数定义（type=ai-inference 时关联）</li>
 * </ol>
 */
public class DbConfigLoader implements AutoCloseable {

    private static final String SQL_JOB =
            "SELECT * FROM td_flink_job WHERE job_name = ? AND status = 1";
    private static final String SQL_PROCESSORS =
            "SELECT * FROM td_pipeline_processor WHERE job_name = ? AND status = 1 ORDER BY step_order";
    private static final String SQL_AI_FUNCTION =
            "SELECT * FROM td_ai_function WHERE function_id = ? AND status = 1";

    private final DbConnectionPool pool;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public DbConfigLoader(DbConfig dbConfig) {
        this.pool = new DbConnectionPool(dbConfig);
    }

    public PipelineConfig load(String jobName) throws Exception {
        PipelineConfig config = new PipelineConfig();
        config.setJobName(jobName);

        try (Connection conn = pool.getConnection()) {
            // 1. 加载作业基础配置
            loadJobConfig(conn, jobName, config);

            // 2. 加载处理器链
            List<ProcessorSpec> processors = loadProcessors(conn, jobName);
            config.setProcessors(processors);
        }
        return config;
    }

    // -------------------------------------------------------------------------
    // Private
    // -------------------------------------------------------------------------

    private void loadJobConfig(Connection conn, String jobName, PipelineConfig config) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(SQL_JOB)) {
            ps.setString(1, jobName);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalArgumentException("作业配置不存在或已禁用: " + jobName);
                }
                config.setParallelism(rs.getInt("parallelism"));
                config.setKafkaConfig(parseKafkaConfig(rs.getString("kafka_config")));
                config.setPaimonConfig(parsePaimonConfig(rs.getString("paimon_config")));
                config.setSchema(parseSchema(rs.getString("schema_config")));
            }
        }
    }

    private List<ProcessorSpec> loadProcessors(Connection conn, String jobName) throws Exception {
        List<ProcessorSpec> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(SQL_PROCESSORS)) {
            ps.setString(1, jobName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ProcessorSpec spec = new ProcessorSpec();
                    spec.setType(rs.getString("processor_type"));
                    spec.setProcessorConfig(rs.getString("processor_config"));

                    if ("ai-inference".equals(spec.getType())) {
                        String functionId = rs.getString("function_id");
                        if (functionId != null) {
                            spec.setAiFunctionSpec(loadAiFunction(conn, functionId));
                        }
                    }
                    result.add(spec);
                }
            }
        }
        return result;
    }

    private AiFunctionSpec loadAiFunction(Connection conn, String functionId) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(SQL_AI_FUNCTION)) {
            ps.setString(1, functionId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalArgumentException("AI函数配置不存在或已禁用: " + functionId);
                }
                AiFunctionSpec spec = new AiFunctionSpec();
                spec.setFunctionId(rs.getString("function_id"));
                spec.setFunctionName(rs.getString("function_name"));
                spec.setFunctionType(rs.getString("function_type"));
                spec.setServiceUrl(rs.getString("service_url"));
                spec.setServiceType(rs.getString("service_type"));
                spec.setModelName(rs.getString("model_name"));
                spec.setInputFields(parseStringList(rs.getString("input_fields")));
                spec.setOutputFields(parseStringList(rs.getString("output_fields")));
                spec.setBatchSize(rs.getInt("batch_size"));
                spec.setMaxWaitMs(rs.getLong("max_wait_ms"));
                spec.setAsyncCapacity(rs.getInt("async_capacity"));
                spec.setFailureStrategy(rs.getString("failure_strategy"));
                String extra = rs.getString("extra_config");
                if (extra != null && !extra.isEmpty()) {
                    spec.setExtraConfig(objectMapper.readValue(extra,
                            new TypeReference<Map<String, Object>>() {}));
                }
                return spec;
            }
        }
    }

    private KafkaConfig parseKafkaConfig(String json) throws Exception {
        JsonNode node = objectMapper.readTree(json);
        KafkaConfig.Builder b = KafkaConfig.builder()
                .bootstrapServers(node.get("bootstrapServers").asText())
                .topic(node.get("topic").asText())
                .groupId(node.path("groupId").asText("flink-ai-group"));
        String offset = node.path("startingOffset").asText("latest");
        if ("earliest".equals(offset))  b.startFromEarliest();
        else if ("latest".equals(offset)) b.startFromLatest();
        else if ("timestamp".equals(offset)) b.startFromTimestamp(node.get("startingTimestamp").asLong());
        return b.build();
    }

    private PaimonConfig parsePaimonConfig(String json) throws Exception {
        JsonNode node = objectMapper.readTree(json);
        PaimonConfig.Builder b = PaimonConfig.builder()
                .warehouse(node.get("warehouse").asText())
                .catalogName(node.path("catalogName").asText("paimon_catalog"))
                .database(node.path("database").asText("default"));
        JsonNode props = node.path("tableProperties");
        if (props.isObject()) {
            props.fields().forEachRemaining(e -> b.tableProperty(e.getKey(), e.getValue().asText()));
        }
        return b.build();
    }

    private SchemaDefinition parseSchema(String json) throws Exception {
        JsonNode node = objectMapper.readTree(json);
        SchemaDefinition.Builder b = SchemaDefinition.builder(node.get("table").asText());
        for (JsonNode fieldNode : node.get("fields")) {
            SchemaField.Builder fb = SchemaField.of(
                    fieldNode.get("name").asText(),
                    FieldType.valueOf(fieldNode.get("type").asText()));
            if (!fieldNode.path("nullable").asBoolean(true)) fb.notNull();
            if ("DECIMAL".equals(fieldNode.path("type").asText())) {
                fb.decimal(fieldNode.path("precision").asInt(10),
                           fieldNode.path("scale").asInt(2));
            }
            b.field(fb.build());
        }
        JsonNode pks = node.path("primaryKeys");
        if (pks.isArray()) {
            List<String> pkList = new ArrayList<>();
            pks.forEach(pk -> pkList.add(pk.asText()));
            b.primaryKey(pkList.toArray(new String[0]));
        }
        JsonNode parts = node.path("partitionKeys");
        if (parts.isArray()) {
            List<String> partList = new ArrayList<>();
            parts.forEach(p -> partList.add(p.asText()));
            b.partitionKey(partList.toArray(new String[0]));
        }
        return b.build();
    }

    @SuppressWarnings("unchecked")
    private List<String> parseStringList(String json) throws Exception {
        if (json == null || json.isEmpty()) return new ArrayList<>();
        return objectMapper.readValue(json, List.class);
    }

    @Override
    public void close() {
        pool.close();
    }
}
