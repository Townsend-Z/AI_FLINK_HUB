package cn.thinkingdata.td.baseserver.operator.engine;

import cn.thinkingdata.td.baseserver.operator.rule.FilterCondition;
import cn.thinkingdata.td.baseserver.operator.rule.FilterRule;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

/**
 * 过滤条件评估器，根据 {@link FilterRule} 对 {@link Row} 进行条件判断。
 */
public class FilterConditionEvaluator implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String[] fieldNames;

    public FilterConditionEvaluator(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    public boolean evaluate(Row row, FilterRule rule) {
        List<FilterCondition> conditions = rule.getConditions();
        if (conditions == null || conditions.isEmpty()) return true;

        boolean isOr = "OR".equalsIgnoreCase(rule.getLogic());
        for (FilterCondition c : conditions) {
            boolean result = evaluateCondition(row, c);
            if (isOr && result)  return true;
            if (!isOr && !result) return false;
        }
        return !isOr; // AND: all passed; OR: none matched
    }

    private boolean evaluateCondition(Row row, FilterCondition c) {
        Object fieldValue = getFieldValue(row, c.getField());
        switch (c.getOp()) {
            case "eq":          return Objects.equals(String.valueOf(fieldValue), String.valueOf(c.getValue()));
            case "ne":          return !Objects.equals(String.valueOf(fieldValue), String.valueOf(c.getValue()));
            case "gt":          return compare(fieldValue, c.getValue()) > 0;
            case "gte":         return compare(fieldValue, c.getValue()) >= 0;
            case "lt":          return compare(fieldValue, c.getValue()) < 0;
            case "lte":         return compare(fieldValue, c.getValue()) <= 0;
            case "in":          return c.getValues() != null && c.getValues().stream()
                                        .anyMatch(v -> Objects.equals(String.valueOf(fieldValue), String.valueOf(v)));
            case "not_in":      return c.getValues() == null || c.getValues().stream()
                                        .noneMatch(v -> Objects.equals(String.valueOf(fieldValue), String.valueOf(v)));
            case "is_null":     return fieldValue == null;
            case "not_null":    return fieldValue != null;
            case "contains":    return fieldValue != null && String.valueOf(fieldValue)
                                        .contains(String.valueOf(c.getValue()));
            case "starts_with": return fieldValue != null && String.valueOf(fieldValue)
                                        .startsWith(String.valueOf(c.getValue()));
            case "ends_with":   return fieldValue != null && String.valueOf(fieldValue)
                                        .endsWith(String.valueOf(c.getValue()));
            default:
                throw new IllegalArgumentException("不支持的过滤操作符: " + c.getOp());
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private int compare(Object a, Object b) {
        if (a == null || b == null) return a == null ? -1 : 1;
        try {
            BigDecimal numA = new BigDecimal(String.valueOf(a));
            BigDecimal numB = new BigDecimal(String.valueOf(b));
            return numA.compareTo(numB);
        } catch (NumberFormatException e) {
            return String.valueOf(a).compareTo(String.valueOf(b));
        }
    }

    private Object getFieldValue(Row row, String fieldName) {
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equals(fieldName)) return row.getField(i);
        }
        return null;
    }
}
