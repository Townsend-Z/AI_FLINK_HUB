package cn.thinkingdata.td.baseserver.ai.mcp;

import cn.thinkingdata.td.baseserver.ai.client.InferenceClient;
import cn.thinkingdata.td.baseserver.ai.config.InferenceConfig;
import cn.thinkingdata.td.baseserver.ai.model.InferenceBatch;
import cn.thinkingdata.td.baseserver.ai.mcp.tools.DbLookupTool;
import cn.thinkingdata.td.baseserver.ai.mcp.tools.ExternalApiTool;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 基于 MCP 协议的 AI 推理客户端。
 *
 * <p>推理流程（Agent Loop）：
 * <ol>
 *   <li>将输入字段组装为 inputData，连同工具 Schema 发往 MCP Server</li>
 *   <li>Server 返回的响应若为 {@code type=tool_call}，则本地执行对应工具并将结果返回 Server</li>
 *   <li>Server 返回 {@code type=final_result} 时，提取推理结果字段</li>
 * </ol>
 */
public class McpAgentClient implements InferenceClient {

    private static final long serialVersionUID = 1L;
    private static final int MAX_AGENT_LOOP = 10;

    private transient McpClient mcpClient;
    private transient McpToolRegistry toolRegistry;

    @Override
    public void init(InferenceConfig config) {
        String mcpUrl = config.getMcpServerUrl();
        if (mcpUrl == null || mcpUrl.isEmpty()) {
            throw new IllegalArgumentException("MCP 推理需要配置 mcpServerUrl");
        }
        this.mcpClient = new McpClient(mcpUrl, config.getAsyncTimeoutMs());
        this.mcpClient.init();

        // 注册默认工具（生产中可根据 extraConfig 动态注册）
        this.toolRegistry = new McpToolRegistry();
        toolRegistry.register(new DbLookupTool());
        toolRegistry.register(new ExternalApiTool());
    }

    @Override
    public List<Map<String, Object>> batchInfer(InferenceBatch batch,
                                                 InferenceConfig config) throws Exception {
        ensureInitialized(config);
        List<Map<String, Object>> results = new ArrayList<>();
        List<Map<String, Object>> toolSchemas = toolRegistry.toToolSchemas();

        for (Row row : batch.getRows()) {
            Map<String, Object> inputData = buildInputData(row, config);
            Map<String, Object> result = runAgentLoop(inputData, toolSchemas, config);
            results.add(result);
        }
        return results;
    }

    // -------------------------------------------------------------------------
    // Private
    // -------------------------------------------------------------------------

    private Map<String, Object> buildInputData(Row row, InferenceConfig config) {
        Map<String, Object> data = new LinkedHashMap<>();
        List<String> inFields = config.getInputFields();
        for (int i = 0; i < inFields.size(); i++) {
            data.put(inFields.get(i), row.getField(i));
        }
        return data;
    }

    private Map<String, Object> runAgentLoop(Map<String, Object> inputData,
                                              List<Map<String, Object>> toolSchemas,
                                              InferenceConfig config) throws Exception {
        Map<String, Object> context = new LinkedHashMap<>(inputData);
        for (int round = 0; round < MAX_AGENT_LOOP; round++) {
            Map<String, Object> response = mcpClient.infer(context, toolSchemas);
            String type = (String) response.getOrDefault("type", "final_result");

            if ("tool_call".equals(type)) {
                // Agent 要求调用工具
                String toolName = (String) response.get("tool_name");
                @SuppressWarnings("unchecked")
                Map<String, Object> toolArgs = (Map<String, Object>) response.get("tool_args");
                Object toolResult = executeTool(toolName, toolArgs);
                context.put("tool_result_" + toolName, toolResult);

            } else {
                // 最终结果，提取 output_fields
                Map<String, Object> finalResult = new LinkedHashMap<>();
                for (String outField : config.getOutputFields()) {
                    finalResult.put(outField, response.get(outField));
                }
                return finalResult;
            }
        }
        throw new RuntimeException("Agent Loop 超过最大轮次 " + MAX_AGENT_LOOP + " 未返回最终结果");
    }

    private Object executeTool(String toolName, Map<String, Object> args) throws Exception {
        McpTool tool = toolRegistry.get(toolName);
        if (tool == null) {
            return Map.of("error", "工具未注册: " + toolName);
        }
        return tool.execute(args);
    }

    private void ensureInitialized(InferenceConfig config) {
        if (mcpClient == null) init(config);
    }

    @Override
    public void close() throws IOException {
        // McpClient 内部 HttpClient 无需显式关闭
    }
}
