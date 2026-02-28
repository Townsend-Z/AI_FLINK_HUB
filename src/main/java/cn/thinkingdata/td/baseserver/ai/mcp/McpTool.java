package cn.thinkingdata.td.baseserver.ai.mcp;

import java.io.Serializable;
import java.util.Map;

/**
 * MCP 工具接口，实现类代表一个可被 AI Agent 调用的工具。
 */
public interface McpTool extends Serializable {

    /** 工具唯一名称，对应 MCP 协议中的 tool name */
    String getName();

    /** 工具功能描述（供 AI Agent 理解何时使用此工具） */
    String getDescription();

    /**
     * 工具参数 JSON Schema（告知 Agent 如何构造调用参数）。
     * 示例：{"type":"object","properties":{"table":{"type":"string"},"query":{"type":"string"}}}
     */
    Map<String, Object> getInputSchema();

    /**
     * 执行工具调用。
     *
     * @param args Agent 传入的参数 Map
     * @return 工具执行结果（将序列化为 JSON 返回给 Agent）
     */
    Object execute(Map<String, Object> args) throws Exception;
}
