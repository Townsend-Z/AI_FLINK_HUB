package cn.thinkingdata.td.baseserver.operator;

import cn.thinkingdata.td.baseserver.operator.rule.AggregateRule;
import cn.thinkingdata.td.baseserver.processor.DataProcessor;
import cn.thinkingdata.td.baseserver.schema.SchemaDefinition;
import cn.thinkingdata.td.baseserver.schema.SchemaField;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.Serializable;

/**
 * 数据库配置驱动的窗口聚合处理器。
 *
 * <p>聚合规则从 DB 的 {@code td_pipeline_processor.processor_config} 字段（JSON）中加载。
 *
 * <p>JSON 示例：
 * <pre>
 * {
 *   "key_fields": ["user_id", "event_type"],
 *   "window": {"type": "tumbling", "size_seconds": 60},
 *   "aggregations": [
 *     {"output": "total_amount", "input": "amount", "op": "sum"},
 *     {"output": "event_count",  "op": "count"},
 *     {"output": "avg_amount",   "input": "amount", "op": "avg"}
 *   ]
 * }
 * </pre>
 */
public class ConfigurableAggregateProcessor implements DataProcessor, Serializable {

    private static final long serialVersionUID = 1L;

    private final AggregateRule rule;
    private final String[] inputFieldNames;

    public ConfigurableAggregateProcessor(AggregateRule rule, SchemaDefinition inputSchema) {
        this.rule            = rule;
        this.inputFieldNames = inputSchema.getFields().stream()
                .map(SchemaField::getName)
                .toArray(String[]::new);
    }

    @Override
    public DataStream<Row> process(DataStream<Row> input) {
        List<String> keyFields = rule.getKeyFields();

        // keyBy: 按关键字段分组
        KeyedStream<Row, String> keyed = input.keyBy(row -> buildKey(row, keyFields));

        // 选择窗口类型并执行聚合
        AggregateRule.WindowSpec window = rule.getWindow();
        long sizeSeconds = window.getSizeSeconds();

        DataStream<Row> windowed;
        if ("sliding".equalsIgnoreCase(window.getType())) {
            long slideSeconds = window.getSlideSeconds();
            windowed = keyed
                    .window(SlidingProcessingTimeWindows.of(
                            Time.seconds(sizeSeconds), Time.seconds(slideSeconds)))
                    .aggregate(new RowAggregateFunction(rule, inputFieldNames))
                    .name("configurable-aggregate[sliding]");
        } else {
            windowed = keyed
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(sizeSeconds)))
                    .aggregate(new RowAggregateFunction(rule, inputFieldNames))
                    .name("configurable-aggregate[tumbling]");
        }
        return windowed;
    }

    private String buildKey(Row row, List<String> keyFields) {
        StringBuilder sb = new StringBuilder();
        for (String kf : keyFields) {
            for (int i = 0; i < inputFieldNames.length; i++) {
                if (inputFieldNames[i].equals(kf)) {
                    sb.append(row.getField(i)).append("|");
                    break;
                }
            }
        }
        return sb.toString();
    }

    // -------------------------------------------------------------------------
    // AggregateFunction 实现
    // -------------------------------------------------------------------------

    /**
     * 中间聚合状态：每个聚合项对应一个 double 累计值。
     */
    private static class AggState {
        Map<String, Double> sums   = new HashMap<>();
        Map<String, Long>   counts = new HashMap<>();
        Map<String, Double> mins   = new HashMap<>();
        Map<String, Double> maxs   = new HashMap<>();
        // 保留最后一条 Row 用于输出 key 字段
        Row lastRow;
    }

    private static class RowAggregateFunction
            implements AggregateFunction<Row, AggState, Row> {

        private final AggregateRule rule;
        private final String[] fieldNames;

        RowAggregateFunction(AggregateRule rule, String[] fieldNames) {
            this.rule       = rule;
            this.fieldNames = fieldNames;
        }

        @Override
        public AggState createAccumulator() { return new AggState(); }

        @Override
        public AggState add(Row row, AggState acc) {
            acc.lastRow = row;
            for (AggregateRule.AggregationSpec agg : rule.getAggregations()) {
                String key = agg.getOutput();
                acc.counts.merge(key, 1L, Long::sum);
                if (agg.getInput() != null) {
                    double val = getDouble(row, agg.getInput());
                    acc.sums.merge(key, val, Double::sum);
                    acc.mins.merge(key, val, Math::min);
                    acc.maxs.merge(key, val, Math::max);
                }
            }
            return acc;
        }

        @Override
        public Row getResult(AggState acc) {
            List<String> keyFields = rule.getKeyFields();
            List<AggregateRule.AggregationSpec> aggs = rule.getAggregations();
            Row out = new Row(keyFields.size() + aggs.size());

            // 输出 key 字段
            for (int i = 0; i < keyFields.size(); i++) {
                out.setField(i, getFieldValue(acc.lastRow, keyFields.get(i)));
            }
            // 输出聚合结果
            for (int i = 0; i < aggs.size(); i++) {
                AggregateRule.AggregationSpec agg = aggs.get(i);
                String key = agg.getOutput();
                long count = acc.counts.getOrDefault(key, 0L);
                out.setField(keyFields.size() + i, computeAgg(agg.getOp(), acc, key, count));
            }
            return out;
        }

        @Override
        public AggState merge(AggState a, AggState b) {
            b.sums.forEach((k, v)   -> a.sums.merge(k, v, Double::sum));
            b.counts.forEach((k, v) -> a.counts.merge(k, v, Long::sum));
            b.mins.forEach((k, v)   -> a.mins.merge(k, v, Math::min));
            b.maxs.forEach((k, v)   -> a.maxs.merge(k, v, Math::max));
            if (b.lastRow != null) a.lastRow = b.lastRow;
            return a;
        }

        private Object computeAgg(String op, AggState acc, String key, long count) {
            switch (op) {
                case "sum":   return acc.sums.getOrDefault(key, 0.0);
                case "count": return count;
                case "avg":   return count == 0 ? 0.0 : acc.sums.getOrDefault(key, 0.0) / count;
                case "min":   return acc.mins.getOrDefault(key, 0.0);
                case "max":   return acc.maxs.getOrDefault(key, 0.0);
                case "count_distinct": return count; // 简化：需生产中用 HyperLogLog 等实现
                default:      return null;
            }
        }

        private double getDouble(Row row, String fieldName) {
            Object val = getFieldValue(row, fieldName);
            return val == null ? 0.0 : Double.parseDouble(String.valueOf(val));
        }

        private Object getFieldValue(Row row, String fieldName) {
            for (int i = 0; i < fieldNames.length; i++) {
                if (fieldNames[i].equals(fieldName)) return row.getField(i);
            }
            return null;
        }
    }
}
