package cn.thinkingdata.td.baseserver.ai.mcp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * MCP 协议通信客户端，支持 HTTP SSE 传输。
 *
 * <p>MCP 协议基于 JSON-RPC 2.0，核心方法：
 * <ul>
 *   <li>{@code initialize}    - 握手协商协议版本与能力</li>
 *   <li>{@code tools/list}    - 获取 Server 提供的工具列表</li>
 *   <li>{@code tools/call}    - 调用指定工具</li>
 *   <li>{@code prompts/get}   - 发起推理请求（含上下文）</li>
 * </ul>
 *
 * <p>HttpClient 懒初始化（不参与 Flink 序列化）。
 */
public class McpClient implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String serverUrl;
    private final long timeoutMs;

    private transient HttpClient httpClient;
    private transient ObjectMapper objectMapper;

    public McpClient(String serverUrl, long timeoutMs) {
        this.serverUrl = serverUrl;
        this.timeoutMs = timeoutMs;
    }

    public void init() {
        this.httpClient  = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(timeoutMs))
                .build();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * 发送 tools/list 请求，获取 Server 上的工具列表。
     */
    public List<Map<String, Object>> listTools() throws Exception {
        Map<String, Object> req = buildJsonRpcRequest("tools/list", Map.of());
        JsonNode resp = send(req);
        return objectMapper.convertValue(resp.get("result").get("tools"),
                new TypeReference<List<Map<String, Object>>>() {});
    }

    /**
     * 调用指定工具。
     *
     * @param toolName 工具名称
     * @param args     工具参数
     * @return 工具执行结果
     */
    public Map<String, Object> callTool(String toolName, Map<String, Object> args) throws Exception {
        Map<String, Object> params = Map.of("name", toolName, "arguments", args);
        Map<String, Object> req    = buildJsonRpcRequest("tools/call", params);
        JsonNode resp = send(req);
        return objectMapper.convertValue(resp.get("result"),
                new TypeReference<Map<String, Object>>() {});
    }

    /**
     * 发起完整推理请求（携带输入数据 + 可用工具列表）。
     *
     * @param inputData   输入特征数据
     * @param toolSchemas 可用工具 Schema（供 Agent 选择调用）
     * @return Agent 响应（可能是 tool_call 或 final_result）
     */
    public Map<String, Object> infer(Map<String, Object> inputData,
                                      List<Map<String, Object>> toolSchemas) throws Exception {
        Map<String, Object> params = Map.of("input", inputData, "tools", toolSchemas);
        Map<String, Object> req    = buildJsonRpcRequest("inference/run", params);
        JsonNode resp = send(req);
        return objectMapper.convertValue(resp.get("result"),
                new TypeReference<Map<String, Object>>() {});
    }

    // -------------------------------------------------------------------------
    // Private
    // -------------------------------------------------------------------------

    private Map<String, Object> buildJsonRpcRequest(String method, Map<String, Object> params) {
        return Map.of(
                "jsonrpc", "2.0",
                "id",      System.currentTimeMillis(),
                "method",  method,
                "params",  params);
    }

    private JsonNode send(Map<String, Object> requestBody) throws Exception {
        ensureInitialized();
        String body = objectMapper.writeValueAsString(requestBody);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(serverUrl + "/mcp"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .timeout(Duration.ofMillis(timeoutMs))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new RuntimeException("MCP Server 返回错误: " + response.statusCode());
        }
        JsonNode respNode = objectMapper.readTree(response.body());
        if (respNode.has("error")) {
            throw new RuntimeException("MCP 调用错误: " + respNode.get("error").toString());
        }
        return respNode;
    }

    private void ensureInitialized() {
        if (httpClient == null) init();
    }
}
