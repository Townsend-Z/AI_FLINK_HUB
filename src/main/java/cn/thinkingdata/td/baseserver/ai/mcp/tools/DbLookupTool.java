package cn.thinkingdata.td.baseserver.ai.mcp.tools;

import cn.thinkingdata.td.baseserver.ai.mcp.McpTool;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 数据库上下文查询工具（占位实现）。
 * 后续可实现为根据字段值查询外部 DB 获取上下文信息，丰富推理输入。
 */
public class DbLookupTool implements McpTool {

    private static final long serialVersionUID = 1L;

    @Override
    public String getName() { return "db-lookup"; }

    @Override
    public String getDescription() {
        return "根据指定主键在数据库中查询记录，返回关联的上下文信息";
    }

    @Override
    public Map<String, Object> getInputSchema() {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        Map<String, Object> props = new LinkedHashMap<>();
        Map<String, Object> tableProp = new LinkedHashMap<>();
        tableProp.put("type", "string");
        tableProp.put("description", "目标表名");
        Map<String, Object> keyProp = new LinkedHashMap<>();
        keyProp.put("type", "string");
        keyProp.put("description", "查询主键值");
        props.put("table", tableProp);
        props.put("key", keyProp);
        schema.put("properties", props);
        schema.put("required", new String[]{"table", "key"});
        return schema;
    }

    @Override
    public Object execute(Map<String, Object> args) throws Exception {
        // TODO: 实现实际的 DB 查询逻辑
        String table = (String) args.get("table");
        String key   = (String) args.get("key");
        return Map.of("status", "not_implemented", "table", table, "key", key);
    }
}
