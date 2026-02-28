package cn.thinkingdata.td.baseserver.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 描述一张 Paimon 目标表的 Schema，包含字段、主键、分区键定义。
 * 同时作为 Kafka 消息反序列化时的字段映射依据。
 */
public class SchemaDefinition implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private final String tableName;
    private final List<SchemaField> fields;
    private final List<String> primaryKeys;
    private final List<String> partitionKeys;

    private SchemaDefinition(Builder builder) {
        this.tableName     = builder.tableName;
        this.fields        = Collections.unmodifiableList(builder.fields);
        this.primaryKeys   = Collections.unmodifiableList(builder.primaryKeys);
        this.partitionKeys = Collections.unmodifiableList(builder.partitionKeys);
    }

    public String getTableName()          { return tableName; }
    public List<SchemaField> getFields()  { return fields; }
    public List<String> getPrimaryKeys()  { return primaryKeys; }
    public List<String> getPartitionKeys(){ return partitionKeys; }

    public static Builder builder(String tableName) { return new Builder(tableName); }

    public static class Builder {
        private final String tableName;
        private final List<SchemaField> fields        = new ArrayList<>();
        private final List<String> primaryKeys        = new ArrayList<>();
        private final List<String> partitionKeys      = new ArrayList<>();

        private Builder(String tableName) { this.tableName = tableName; }

        public Builder field(SchemaField field)      { this.fields.add(field); return this; }
        public Builder primaryKey(String... keys)    { Collections.addAll(this.primaryKeys, keys); return this; }
        public Builder partitionKey(String... keys)  { Collections.addAll(this.partitionKeys, keys); return this; }

        public SchemaDefinition build() { return new SchemaDefinition(this); }
    }
}
