package cn.thinkingdata.td.baseserver.schema;

/**
 * 单个字段的 Schema 定义
 */
public class SchemaField implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final FieldType type;
    private final boolean nullable;
    /** 仅 DECIMAL 类型使用 */
    private final int precision;
    private final int scale;

    private SchemaField(Builder builder) {
        this.name = builder.name;
        this.type = builder.type;
        this.nullable = builder.nullable;
        this.precision = builder.precision;
        this.scale = builder.scale;
    }

    public static Builder of(String name, FieldType type) {
        return new Builder(name, type);
    }

    public String getName()     { return name; }
    public FieldType getType()  { return type; }
    public boolean isNullable() { return nullable; }
    public int getPrecision()   { return precision; }
    public int getScale()       { return scale; }

    public static class Builder {
        private final String name;
        private final FieldType type;
        private boolean nullable = true;
        private int precision = 10;
        private int scale = 2;

        private Builder(String name, FieldType type) {
            this.name = name;
            this.type = type;
        }

        public Builder notNull()                        { this.nullable = false; return this; }
        public Builder decimal(int precision, int scale){ this.precision = precision; this.scale = scale; return this; }

        public SchemaField build() { return new SchemaField(this); }
    }
}
