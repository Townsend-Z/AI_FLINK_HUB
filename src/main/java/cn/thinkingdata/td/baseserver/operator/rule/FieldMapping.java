package cn.thinkingdata.td.baseserver.operator.rule;

import java.io.Serializable;
import java.util.List;

/**
 * 单个字段映射/转换规则
 *
 * <p>支持操作 (op)：
 * <ul>
 *   <li>rename       - 字段重命名（input → output）</li>
 *   <li>add/sub/mul/divide - 数值运算（input op operand）</li>
 *   <li>concat       - 字符串拼接（inputs 列表 + separator）</li>
 *   <li>substring    - 截取子串（input, start, length）</li>
 *   <li>upper/lower  - 大小写转换</li>
 *   <li>trim         - 去除首尾空格</li>
 *   <li>date_format  - 日期格式化（input, format）</li>
 *   <li>if_else      - 条件赋值（condition, then_value, else_value）</li>
 *   <li>literal      - 常量赋值（value）</li>
 *   <li>type_cast    - 类型转换（input, target_type: STRING/INT/LONG/DOUBLE/BOOLEAN）</li>
 * </ul>
 */
public class FieldMapping implements Serializable {

    /** 输出字段名 */
    private String output;
    /** 主输入字段名（单输入操作使用） */
    private String input;
    /** 多输入字段列表（concat 等多输入操作使用） */
    private List<String> inputs;
    /** 操作类型 */
    private String op;
    /** 数值运算的操作数 */
    private Double operand;
    /** concat 分隔符 */
    private String separator = "";
    /** substring 起始位置 */
    private Integer start;
    /** substring 长度 */
    private Integer length;
    /** date_format 格式字符串 */
    private String format;
    /** if_else 条件 */
    private FilterCondition condition;
    /** if_else 为真时的值 */
    private Object thenValue;
    /** if_else 为假时的值 */
    private Object elseValue;
    /** literal 常量值 */
    private Object value;
    /** type_cast 目标类型 */
    private String targetType;

    public FieldMapping() {}

    public String getOutput()              { return output; }
    public void setOutput(String v)        { this.output = v; }
    public String getInput()               { return input; }
    public void setInput(String v)         { this.input = v; }
    public List<String> getInputs()        { return inputs; }
    public void setInputs(List<String> v)  { this.inputs = v; }
    public String getOp()                  { return op; }
    public void setOp(String v)            { this.op = v; }
    public Double getOperand()             { return operand; }
    public void setOperand(Double v)       { this.operand = v; }
    public String getSeparator()           { return separator; }
    public void setSeparator(String v)     { this.separator = v; }
    public Integer getStart()              { return start; }
    public void setStart(Integer v)        { this.start = v; }
    public Integer getLength()             { return length; }
    public void setLength(Integer v)       { this.length = v; }
    public String getFormat()              { return format; }
    public void setFormat(String v)        { this.format = v; }
    public FilterCondition getCondition()  { return condition; }
    public void setCondition(FilterCondition v){ this.condition = v; }
    public Object getThenValue()           { return thenValue; }
    public void setThenValue(Object v)     { this.thenValue = v; }
    public Object getElseValue()           { return elseValue; }
    public void setElseValue(Object v)     { this.elseValue = v; }
    public Object getValue()               { return value; }
    public void setValue(Object v)         { this.value = v; }
    public String getTargetType()          { return targetType; }
    public void setTargetType(String v)    { this.targetType = v; }
}
