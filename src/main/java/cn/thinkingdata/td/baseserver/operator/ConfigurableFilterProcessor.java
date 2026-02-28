package cn.thinkingdata.td.baseserver.operator;

import cn.thinkingdata.td.baseserver.operator.engine.FilterConditionEvaluator;
import cn.thinkingdata.td.baseserver.operator.rule.FilterRule;
import cn.thinkingdata.td.baseserver.processor.DataProcessor;
import cn.thinkingdata.td.baseserver.schema.SchemaDefinition;
import cn.thinkingdata.td.baseserver.schema.SchemaField;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * 数据库配置驱动的过滤处理器。
 *
 * <p>过滤规则从 DB 的 {@code td_pipeline_processor.processor_config} 字段（JSON）中加载。
 *
 * <p>JSON 示例：
 * <pre>
 * {
 *   "logic": "AND",
 *   "conditions": [
 *     {"field": "amount",     "op": "gt",      "value": 100},
 *     {"field": "event_type", "op": "in",      "values": ["click","purchase"]},
 *     {"field": "user_id",    "op": "not_null"}
 *   ]
 * }
 * </pre>
 */
public class ConfigurableFilterProcessor implements DataProcessor, Serializable {

    private static final long serialVersionUID = 1L;

    private final FilterRule rule;
    private final FilterConditionEvaluator evaluator;

    public ConfigurableFilterProcessor(FilterRule rule, SchemaDefinition schema) {
        this.rule      = rule;
        String[] names = schema.getFields().stream()
                .map(SchemaField::getName)
                .toArray(String[]::new);
        this.evaluator = new FilterConditionEvaluator(names);
    }

    @Override
    public DataStream<Row> process(DataStream<Row> input) {
        return input.filter(row -> evaluator.evaluate(row, rule)).name("configurable-filter");
    }
}
