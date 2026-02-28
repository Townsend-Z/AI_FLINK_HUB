package cn.thinkingdata.td.baseserver.operator.rule;

import java.io.Serializable;
import java.util.List;

/**
 * 窗口聚合规则
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
public class AggregateRule implements Serializable {

    private List<String> keyFields;
    private WindowSpec window;
    private List<AggregationSpec> aggregations;

    public AggregateRule() {}

    public List<String> getKeyFields()             { return keyFields; }
    public void setKeyFields(List<String> v)       { this.keyFields = v; }
    public WindowSpec getWindow()                  { return window; }
    public void setWindow(WindowSpec v)            { this.window = v; }
    public List<AggregationSpec> getAggregations() { return aggregations; }
    public void setAggregations(List<AggregationSpec> v){ this.aggregations = v; }

    /** 窗口配置 */
    public static class WindowSpec implements Serializable {
        /** "tumbling" | "sliding" */
        private String type = "tumbling";
        /** 窗口大小（秒） */
        private long sizeSeconds = 60;
        /** 滑动步长（秒，sliding 类型使用） */
        private long slideSeconds = 10;

        public String getType()             { return type; }
        public void setType(String v)       { this.type = v; }
        public long getSizeSeconds()        { return sizeSeconds; }
        public void setSizeSeconds(long v)  { this.sizeSeconds = v; }
        public long getSlideSeconds()       { return slideSeconds; }
        public void setSlideSeconds(long v) { this.slideSeconds = v; }
    }

    /** 单个聚合项配置 */
    public static class AggregationSpec implements Serializable {
        private String output;
        private String input;
        /** "sum" | "count" | "avg" | "min" | "max" | "count_distinct" */
        private String op;

        public String getOutput()        { return output; }
        public void setOutput(String v)  { this.output = v; }
        public String getInput()         { return input; }
        public void setInput(String v)   { this.input = v; }
        public String getOp()            { return op; }
        public void setOp(String v)      { this.op = v; }
    }
}
