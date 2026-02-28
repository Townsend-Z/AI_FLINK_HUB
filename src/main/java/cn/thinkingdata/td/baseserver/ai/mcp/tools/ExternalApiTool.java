package cn.thinkingdata.td.baseserver.ai.mcp.tools;

import cn.thinkingdata.td.baseserver.ai.mcp.McpTool;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 外部 API 调用工具（占位实现）。
 * 后续可实现为调用外部 REST API 获取实时数据（如汇率、风控评分等）。
 */
public class ExternalApiTool implements McpTool {

    private static final long serialVersionUID = 1L;

    @Override
    public String getName() { return "external-api"; }

    @Override
    public String getDescription() {
        return "调用外部 REST API 获取实时数据，如汇率、用户画像、风控评分等";
    }

    @Override
    public Map<String, Object> getInputSchema() {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        Map<String, Object> props = new LinkedHashMap<>();
        Map<String, Object> urlProp = new LinkedHashMap<>();
        urlProp.put("type", "string");
        urlProp.put("description", "API 地址");
        Map<String, Object> paramsProp = new LinkedHashMap<>();
        paramsProp.put("type", "object");
        paramsProp.put("description", "请求参数");
        props.put("url", urlProp);
        props.put("params", paramsProp);
        schema.put("properties", props);
        schema.put("required", new String[]{"url"});
        return schema;
    }

    @Override
    public Object execute(Map<String, Object> args) throws Exception {
        // TODO: 实现实际的 HTTP API 调用逻辑
        return Map.of("status", "not_implemented", "url", args.get("url"));
    }
}
