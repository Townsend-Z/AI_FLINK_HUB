package cn.thinkingdata.td.baseserver.ai.factory;

import cn.thinkingdata.td.baseserver.ai.client.HttpInferenceClient;
import cn.thinkingdata.td.baseserver.ai.client.InferenceClient;
import cn.thinkingdata.td.baseserver.ai.config.InferenceConfig;
import cn.thinkingdata.td.baseserver.ai.mcp.McpAgentClient;
import cn.thinkingdata.td.baseserver.ai.processor.AiInferenceProcessor;
import cn.thinkingdata.td.baseserver.config.pipeline.AiFunctionSpec;
import cn.thinkingdata.td.baseserver.processor.DataProcessor;

/**
 * AI 推理算子工厂，根据 {@link AiFunctionSpec#getFunctionType()} 创建对应的 {@link DataProcessor}。
 *
 * <p>内置支持：
 * <ul>
 *   <li>{@code "http"} → {@link HttpInferenceClient}（Triton/TorchServe REST API）</li>
 *   <li>{@code "mcp"}  → {@link McpAgentClient}（MCP Agent Loop + 工具调用链）</li>
 * </ul>
 */
public class AiFunctionFactory {

    private AiFunctionFactory() {}

    public static DataProcessor create(AiFunctionSpec spec) {
        InferenceConfig config = InferenceConfig.from(spec);
        InferenceClient client = createClient(spec.getFunctionType());
        return new AiInferenceProcessor(config, client);
    }

    private static InferenceClient createClient(String functionType) {
        if (functionType == null) {
            throw new IllegalArgumentException("function_type 不能为空");
        }
        switch (functionType.toLowerCase()) {
            case "http":  return new HttpInferenceClient();
            case "mcp":   return new McpAgentClient();
            default:
                throw new IllegalArgumentException(
                        "不支持的 function_type: " + functionType
                        + "，当前内置支持: http, mcp");
        }
    }
}
