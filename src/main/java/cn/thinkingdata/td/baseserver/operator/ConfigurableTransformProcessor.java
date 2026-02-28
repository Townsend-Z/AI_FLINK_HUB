package cn.thinkingdata.td.baseserver.operator;

import cn.thinkingdata.td.baseserver.operator.engine.FieldTransformExecutor;
import cn.thinkingdata.td.baseserver.operator.rule.TransformRule;
import cn.thinkingdata.td.baseserver.processor.DataProcessor;
import cn.thinkingdata.td.baseserver.schema.SchemaDefinition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * 数据库配置驱动的字段转换处理器。
 *
 * <p>转换规则从 DB 的 {@code td_pipeline_processor.processor_config} 字段（JSON）中加载。
 *
 * <p>JSON 示例：
 * <pre>
 * {
 *   "mappings": [
 *     {"output": "amount_usd", "input": "amount",             "op": "divide", "operand": 7.3},
 *     {"output": "full_name",  "inputs": ["first","last"],    "op": "concat", "separator": " "},
 *     {"output": "level",      "op": "if_else",
 *      "condition": {"field": "amount", "op": "gte", "value": 1000},
 *      "then_value": "vip", "else_value": "normal"},
 *     {"output": "event_date", "input": "event_time",         "op": "date_format", "format": "yyyy-MM-dd"},
 *     {"output": "score",      "input": "raw_score",          "op": "type_cast", "target_type": "DOUBLE"}
 *   ]
 * }
 * </pre>
 */
public class ConfigurableTransformProcessor implements DataProcessor, Serializable {

    private static final long serialVersionUID = 1L;

    private final TransformRule rule;
    private final FieldTransformExecutor executor;

    /**
     * @param rule         转换规则（从 DB 配置解析而来）
     * @param inputSchema  输入 Row 的 Schema（字段顺序）
     * @param outputSchema 输出 Row 的 Schema（包含新增字段）
     */
    public ConfigurableTransformProcessor(TransformRule rule,
                                           SchemaDefinition inputSchema,
                                           SchemaDefinition outputSchema) {
        this.rule     = rule;
        this.executor = new FieldTransformExecutor(inputSchema, outputSchema);
    }

    /** 当输入输出 Schema 相同时使用此构造（只是修改/计算现有字段） */
    public ConfigurableTransformProcessor(TransformRule rule, SchemaDefinition schema) {
        this(rule, schema, schema);
    }

    @Override
    public DataStream<Row> process(DataStream<Row> input) {
        return input.map(row -> executor.execute(row, rule)).name("configurable-transform");
    }
}
