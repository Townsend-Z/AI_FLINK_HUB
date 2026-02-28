package cn.thinkingdata.td.baseserver.operator.rule;

import java.io.Serializable;
import java.util.List;

/**
 * 单个过滤条件
 * 支持操作符：eq, ne, gt, gte, lt, lte, in, not_in, is_null, not_null, contains, starts_with, ends_with
 */
public class FilterCondition implements Serializable {

    private String field;
    private String op;
    /** 单值比较时使用 */
    private Object value;
    /** in / not_in 时使用 */
    private List<Object> values;

    public FilterCondition() {}

    public String getField()         { return field; }
    public void setField(String v)   { this.field = v; }
    public String getOp()            { return op; }
    public void setOp(String v)      { this.op = v; }
    public Object getValue()         { return value; }
    public void setValue(Object v)   { this.value = v; }
    public List<Object> getValues()  { return values; }
    public void setValues(List<Object> v){ this.values = v; }
}
