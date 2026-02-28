package cn.thinkingdata.td.baseserver.serialization;

import cn.thinkingdata.td.baseserver.schema.SchemaDefinition;
import cn.thinkingdata.td.baseserver.schema.SchemaField;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 基于 SchemaDefinition 将 Kafka JSON 消息反序列化为 Flink Row。
 * 字段按 schema 中定义的顺序映射，缺失字段填 null。
 */
public class SchemaBasedDeserializer implements DeserializationSchema<Row> {

    private final SchemaDefinition schema;
    private transient ObjectMapper objectMapper;

    public SchemaBasedDeserializer(SchemaDefinition schema) {
        this.schema = schema;
    }

    @Override
    public void open(InitializationContext context) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        JsonNode root = objectMapper.readTree(message);
        Row row = new Row(schema.getFields().size());

        for (int i = 0; i < schema.getFields().size(); i++) {
            SchemaField field = schema.getFields().get(i);
            JsonNode node = root.get(field.getName());
            row.setField(i, convertValue(node, field));
        }
        return row;
    }

    private Object convertValue(JsonNode node, SchemaField field) {
        if (node == null || node.isNull()) {
            return null;
        }
        switch (field.getType()) {
            case STRING:    return node.asText();
            case INT:       return node.asInt();
            case LONG:      return node.asLong();
            case FLOAT:     return (float) node.asDouble();
            case DOUBLE:    return node.asDouble();
            case BOOLEAN:   return node.asBoolean();
            case DECIMAL:   return new BigDecimal(node.asText());
            case DATE:      return LocalDate.parse(node.asText(), DateTimeFormatter.ISO_LOCAL_DATE);
            case TIMESTAMP: return LocalDateTime.parse(node.asText(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            default:        return node.asText();
        }
    }

    @Override
    public boolean isEndOfStream(Row row) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        int size = schema.getFields().size();
        String[] names = new String[size];
        TypeInformation<?>[] types = new TypeInformation<?>[size];

        for (int i = 0; i < size; i++) {
            SchemaField field = schema.getFields().get(i);
            names[i] = field.getName();
            types[i] = toTypeInfo(field);
        }
        return Types.ROW_NAMED(names, types);
    }

    private TypeInformation<?> toTypeInfo(SchemaField field) {
        switch (field.getType()) {
            case STRING:    return Types.STRING;
            case INT:       return Types.INT;
            case LONG:      return Types.LONG;
            case FLOAT:     return Types.FLOAT;
            case DOUBLE:    return Types.DOUBLE;
            case BOOLEAN:   return Types.BOOLEAN;
            case DECIMAL:   return Types.BIG_DEC;
            case DATE:      return Types.LOCAL_DATE;
            case TIMESTAMP: return Types.LOCAL_DATE_TIME;
            default:        return Types.STRING;
        }
    }
}
