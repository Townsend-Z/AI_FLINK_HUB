package cn.thinkingdata.td.baseserver.config.pipeline;

import java.util.List;
import java.util.Map;

/**
 * AI 函数规格，从 td_ai_function 表映射而来。
 */
public class AiFunctionSpec {

    private String functionId;
    private String functionName;
    /** "http" | "mcp" */
    private String functionType;
    private String serviceUrl;
    /** "triton" | "torchserve" | "custom" */
    private String serviceType;
    private String modelName;
    private List<String> inputFields;
    private List<String> outputFields;
    private int batchSize     = 64;
    private long maxWaitMs    = 100L;
    private int asyncCapacity = 100;
    private long asyncTimeoutMs = 5000L;
    /** "SKIP" | "PASS_THROUGH" | "FAIL" */
    private String failureStrategy = "PASS_THROUGH";
    /** 附加配置（MCP 工具列表、HTTP 请求头等） */
    private Map<String, Object> extraConfig;

    public AiFunctionSpec() {}

    public String getFunctionId()          { return functionId; }
    public void setFunctionId(String v)    { this.functionId = v; }
    public String getFunctionName()        { return functionName; }
    public void setFunctionName(String v)  { this.functionName = v; }
    public String getFunctionType()        { return functionType; }
    public void setFunctionType(String v)  { this.functionType = v; }
    public String getServiceUrl()          { return serviceUrl; }
    public void setServiceUrl(String v)    { this.serviceUrl = v; }
    public String getServiceType()         { return serviceType; }
    public void setServiceType(String v)   { this.serviceType = v; }
    public String getModelName()           { return modelName; }
    public void setModelName(String v)     { this.modelName = v; }
    public List<String> getInputFields()   { return inputFields; }
    public void setInputFields(List<String> v)  { this.inputFields = v; }
    public List<String> getOutputFields()  { return outputFields; }
    public void setOutputFields(List<String> v) { this.outputFields = v; }
    public int getBatchSize()              { return batchSize; }
    public void setBatchSize(int v)        { this.batchSize = v; }
    public long getMaxWaitMs()             { return maxWaitMs; }
    public void setMaxWaitMs(long v)       { this.maxWaitMs = v; }
    public int getAsyncCapacity()          { return asyncCapacity; }
    public void setAsyncCapacity(int v)    { this.asyncCapacity = v; }
    public long getAsyncTimeoutMs()        { return asyncTimeoutMs; }
    public void setAsyncTimeoutMs(long v)  { this.asyncTimeoutMs = v; }
    public String getFailureStrategy()     { return failureStrategy; }
    public void setFailureStrategy(String v){ this.failureStrategy = v; }
    public Map<String, Object> getExtraConfig()          { return extraConfig; }
    public void setExtraConfig(Map<String, Object> v)    { this.extraConfig = v; }
}
