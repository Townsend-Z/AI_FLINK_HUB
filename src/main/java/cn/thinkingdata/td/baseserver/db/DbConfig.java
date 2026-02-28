package cn.thinkingdata.td.baseserver.db;

/**
 * 数据库连接配置
 */
public class DbConfig {

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final int maximumPoolSize;
    private final int connectionTimeoutMs;

    private DbConfig(Builder builder) {
        this.jdbcUrl            = builder.jdbcUrl;
        this.username           = builder.username;
        this.password           = builder.password;
        this.maximumPoolSize    = builder.maximumPoolSize;
        this.connectionTimeoutMs = builder.connectionTimeoutMs;
    }

    public static DbConfig of(String jdbcUrl, String username, String password) {
        return builder().jdbcUrl(jdbcUrl).username(username).password(password).build();
    }

    public String getJdbcUrl()            { return jdbcUrl; }
    public String getUsername()           { return username; }
    public String getPassword()           { return password; }
    public int getMaximumPoolSize()       { return maximumPoolSize; }
    public int getConnectionTimeoutMs()   { return connectionTimeoutMs; }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private String jdbcUrl;
        private String username = "";
        private String password = "";
        private int maximumPoolSize    = 10;
        private int connectionTimeoutMs = 30000;

        public Builder jdbcUrl(String v)             { this.jdbcUrl = v; return this; }
        public Builder username(String v)            { this.username = v; return this; }
        public Builder password(String v)            { this.password = v; return this; }
        public Builder maximumPoolSize(int v)        { this.maximumPoolSize = v; return this; }
        public Builder connectionTimeoutMs(int v)    { this.connectionTimeoutMs = v; return this; }

        public DbConfig build() { return new DbConfig(this); }
    }
}
