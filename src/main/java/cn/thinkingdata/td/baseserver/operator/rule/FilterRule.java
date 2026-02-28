package cn.thinkingdata.td.baseserver.operator.rule;

import java.io.Serializable;
import java.util.List;

/**
 * 过滤规则集，包含一组条件和逻辑运算符（AND/OR）。
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
public class FilterRule implements Serializable {

    /** "AND"（默认）或 "OR" */
    private String logic = "AND";
    private List<FilterCondition> conditions;

    public FilterRule() {}

    public String getLogic()                       { return logic; }
    public void setLogic(String v)                 { this.logic = v; }
    public List<FilterCondition> getConditions()   { return conditions; }
    public void setConditions(List<FilterCondition> v){ this.conditions = v; }
}
