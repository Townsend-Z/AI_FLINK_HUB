package cn.thinkingdata.td.baseserver.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Paimon Catalog / 目标表配置
 */
public class PaimonConfig {

    private final String catalogName;
    /** Paimon warehouse 路径，支持本地/HDFS/S3 */
    private final String warehouse;
    private final String database;
    /** 写入 Paimon 时的附加表属性，如 changelog-producer、bucket 等 */
    private final Map<String, String> tableProperties;

    private PaimonConfig(Builder builder) {
        this.catalogName = builder.catalogName;
        this.warehouse = builder.warehouse;
        this.database = builder.database;
        this.tableProperties = Collections.unmodifiableMap(builder.tableProperties);
    }

    public String getCatalogName()             { return catalogName; }
    public String getWarehouse()               { return warehouse; }
    public String getDatabase()                { return database; }
    public Map<String, String> getTableProperties() { return tableProperties; }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private String catalogName = "paimon_catalog";
        private String warehouse;
        private String database   = "default";
        private Map<String, String> tableProperties = new HashMap<>();

        public Builder catalogName(String v)  { this.catalogName = v; return this; }
        public Builder warehouse(String v)    { this.warehouse = v; return this; }
        public Builder database(String v)     { this.database = v; return this; }
        public Builder tableProperty(String k, String v) { this.tableProperties.put(k, v); return this; }

        public PaimonConfig build() { return new PaimonConfig(this); }
    }
}
