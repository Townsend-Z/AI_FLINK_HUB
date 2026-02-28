package cn.thinkingdata.td.baseserver.operator.engine;

import cn.thinkingdata.td.baseserver.operator.rule.FieldMapping;
import cn.thinkingdata.td.baseserver.operator.rule.TransformRule;
import cn.thinkingdata.td.baseserver.schema.SchemaDefinition;
import cn.thinkingdata.td.baseserver.schema.SchemaField;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 字段转换执行器，按照 {@link TransformRule} 对 Row 进行字段映射和计算。
 *
 * <p>执行后返回的 Row 字段顺序与 {@code outputSchema} 的字段顺序一致：
 * 未被映射规则覆盖的字段会从 inputRow 中按名称复制。
 */
public class FieldTransformExecutor implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String[] inputFieldNames;
    private final String[] outputFieldNames;

    public FieldTransformExecutor(SchemaDefinition inputSchema, SchemaDefinition outputSchema) {
        List<SchemaField> inFields  = inputSchema.getFields();
        List<SchemaField> outFields = outputSchema.getFields();
        this.inputFieldNames  = inFields.stream().map(SchemaField::getName).toArray(String[]::new);
        this.outputFieldNames = outFields.stream().map(SchemaField::getName).toArray(String[]::new);
    }

    public Row execute(Row inputRow, TransformRule rule) {
        Row outputRow = new Row(outputFieldNames.length);

        // 先将 input 字段按名称复制到 output（同名字段直接透传）
        for (int i = 0; i < outputFieldNames.length; i++) {
            outputRow.setField(i, getInputFieldValue(inputRow, outputFieldNames[i]));
        }

        // 再执行 mapping 规则（覆盖同名字段，填充新计算字段）
        if (rule.getMappings() != null) {
            for (FieldMapping mapping : rule.getMappings()) {
                Object result = applyMapping(inputRow, mapping);
                setOutputField(outputRow, mapping.getOutput(), result);
            }
        }
        return outputRow;
    }

    // -------------------------------------------------------------------------
    // Private：各操作的执行逻辑
    // -------------------------------------------------------------------------

    private Object applyMapping(Row row, FieldMapping m) {
        switch (m.getOp()) {
            case "rename":      return getInputFieldValue(row, m.getInput());
            case "add":         return arithmetic(row, m, (a, b) -> a + b);
            case "sub":         return arithmetic(row, m, (a, b) -> a - b);
            case "mul":         return arithmetic(row, m, (a, b) -> a * b);
            case "divide":      return arithmetic(row, m, (a, b) -> b == 0 ? null : a / b);
            case "concat":      return concat(row, m);
            case "substring":   return substring(row, m);
            case "upper":       return toStr(getInputFieldValue(row, m.getInput())).toUpperCase();
            case "lower":       return toStr(getInputFieldValue(row, m.getInput())).toLowerCase();
            case "trim":        return toStr(getInputFieldValue(row, m.getInput())).trim();
            case "date_format":  return dateFormat(row, m);
            case "if_else":     return ifElse(row, m);
            case "literal":     return m.getValue();
            case "type_cast":   return typeCast(row, m);
            default:
                throw new IllegalArgumentException("不支持的 transform 操作: " + m.getOp());
        }
    }

    @FunctionalInterface
    interface BiDoubleFunc { Double apply(double a, double b); }

    private Object arithmetic(Row row, FieldMapping m, BiDoubleFunc op) {
        Object val = getInputFieldValue(row, m.getInput());
        if (val == null || m.getOperand() == null) return null;
        return op.apply(Double.parseDouble(String.valueOf(val)), m.getOperand());
    }

    private Object concat(Row row, FieldMapping m) {
        StringBuilder sb = new StringBuilder();
        List<String> inputs = m.getInputs();
        String sep = m.getSeparator() != null ? m.getSeparator() : "";
        for (int i = 0; i < inputs.size(); i++) {
            if (i > 0) sb.append(sep);
            sb.append(toStr(getInputFieldValue(row, inputs.get(i))));
        }
        return sb.toString();
    }

    private Object substring(Row row, FieldMapping m) {
        String s = toStr(getInputFieldValue(row, m.getInput()));
        int start  = m.getStart() != null ? m.getStart() : 0;
        int length = m.getLength() != null ? m.getLength() : s.length();
        int end = Math.min(start + length, s.length());
        return start >= s.length() ? "" : s.substring(start, end);
    }

    private Object dateFormat(Row row, FieldMapping m) {
        Object val = getInputFieldValue(row, m.getInput());
        if (val == null) return null;
        try {
            String fmt = m.getFormat() != null ? m.getFormat() : "yyyy-MM-dd";
            SimpleDateFormat sdf = new SimpleDateFormat(fmt);
            if (val instanceof Date)   return sdf.format(val);
            if (val instanceof Long)   return sdf.format(new Date((Long) val));
            return sdf.format(sdf.parse(String.valueOf(val)));
        } catch (Exception e) {
            return String.valueOf(val);
        }
    }

    private Object ifElse(Row row, FieldMapping m) {
        FilterConditionEvaluator evaluator = new FilterConditionEvaluator(inputFieldNames);
        cn.thinkingdata.td.baseserver.operator.rule.FilterRule rule =
                new cn.thinkingdata.td.baseserver.operator.rule.FilterRule();
        rule.setConditions(java.util.Collections.singletonList(m.getCondition()));
        return evaluator.evaluate(row, rule) ? m.getThenValue() : m.getElseValue();
    }

    private Object typeCast(Row row, FieldMapping m) {
        Object val = getInputFieldValue(row, m.getInput());
        if (val == null) return null;
        String s = String.valueOf(val);
        switch (m.getTargetType() == null ? "STRING" : m.getTargetType().toUpperCase()) {
            case "STRING":  return s;
            case "INT":     return Integer.parseInt(s);
            case "LONG":    return Long.parseLong(s);
            case "FLOAT":   return Float.parseFloat(s);
            case "DOUBLE":  return Double.parseDouble(s);
            case "BOOLEAN": return Boolean.parseBoolean(s);
            default:        return s;
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Object getInputFieldValue(Row row, String fieldName) {
        for (int i = 0; i < inputFieldNames.length; i++) {
            if (inputFieldNames[i].equals(fieldName)) return row.getField(i);
        }
        return null;
    }

    private void setOutputField(Row row, String fieldName, Object value) {
        for (int i = 0; i < outputFieldNames.length; i++) {
            if (outputFieldNames[i].equals(fieldName)) {
                row.setField(i, value);
                return;
            }
        }
    }

    private String toStr(Object o) {
        return o == null ? "" : String.valueOf(o);
    }
}
