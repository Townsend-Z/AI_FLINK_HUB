package cn.thinkingdata.td.baseserver.operator.rule;

import java.io.Serializable;
import java.util.List;

/**
 * 转换规则集
 *
 * <p>JSON 示例：
 * <pre>
 * {
 *   "mappings": [
 *     {"output": "amount_usd", "input": "amount",   "op": "divide",  "operand": 7.3},
 *     {"output": "full_name",  "inputs": ["fn","ln"],"op": "concat",  "separator": " "},
 *     {"output": "level",      "op": "if_else",
 *      "condition": {"field": "amount", "op": "gte", "value": 1000},
 *      "then_value": "vip", "else_value": "normal"}
 *   ]
 * }
 * </pre>
 */
public class TransformRule implements Serializable {

    private List<FieldMapping> mappings;

    public TransformRule() {}

    public List<FieldMapping> getMappings()       { return mappings; }
    public void setMappings(List<FieldMapping> v) { this.mappings = v; }
}
