package cn.thinkingdata.td.baseserver.ai.mcp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * MCP 工具注册中心，管理所有可被 AI Agent 调用的工具。
 */
public class McpToolRegistry implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, McpTool> tools = new LinkedHashMap<>();

    public void register(McpTool tool) {
        tools.put(tool.getName(), tool);
    }

    public McpTool get(String name) {
        return tools.get(name);
    }

    public boolean contains(String name) {
        return tools.containsKey(name);
    }

    public List<McpTool> getAll() {
        return Collections.unmodifiableList(new ArrayList<>(tools.values()));
    }

    /**
     * 生成工具列表的 JSON Schema 描述，发送给 MCP Server 注册。
     */
    public List<Map<String, Object>> toToolSchemas() {
        List<Map<String, Object>> schemas = new ArrayList<>();
        for (McpTool tool : tools.values()) {
            Map<String, Object> schema = new LinkedHashMap<>();
            schema.put("name", tool.getName());
            schema.put("description", tool.getDescription());
            schema.put("inputSchema", tool.getInputSchema());
            schemas.add(schema);
        }
        return schemas;
    }
}
