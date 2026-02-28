package cn.thinkingdata.td.baseserver.ai.config;

import cn.thinkingdata.td.baseserver.config.pipeline.AiFunctionSpec;

import java.io.Serializable;
import java.util.List;

/**
 * 运行时推理配置，由 {@link AiFunctionSpec} 转换而来。
 */
public class InferenceConfig implements Serializable {

    private String functionId;
    private String functionName;
    private String functionType;
    private String serviceUrl;
    private String serviceType;
    private String modelName;
    private List<String> inputFields;
    private List<String> outputFields;
    private int batchSize     = 64;
    private long maxWaitMs    = 100L;
    private int asyncCapacity = 100;
    private long asyncTimeoutMs = 5000L;
    private FailureStrategy failureStrategy = FailureStrategy.PASS_THROUGH;
    private String mcpServerUrl;

    private InferenceConfig() {}

    /** 从 AiFunctionSpec 构建 */
    public static InferenceConfig from(AiFunctionSpec spec) {
        InferenceConfig c = new InferenceConfig();
        c.functionId      = spec.getFunctionId();
        c.functionName    = spec.getFunctionName();
        c.functionType    = spec.getFunctionType();
        c.serviceUrl      = spec.getServiceUrl();
        c.serviceType     = spec.getServiceType();
        c.modelName       = spec.getModelName();
        c.inputFields     = spec.getInputFields();
        c.outputFields    = spec.getOutputFields();
        c.batchSize       = spec.getBatchSize();
        c.maxWaitMs       = spec.getMaxWaitMs();
        c.asyncCapacity   = spec.getAsyncCapacity();
        c.asyncTimeoutMs  = spec.getAsyncTimeoutMs();
        c.failureStrategy = FailureStrategy.valueOf(spec.getFailureStrategy());
        if (spec.getExtraConfig() != null) {
            Object mcpUrl = spec.getExtraConfig().get("mcpServerUrl");
            if (mcpUrl != null) c.mcpServerUrl = mcpUrl.toString();
        }
        return c;
    }

    public String getFunctionId()          { return functionId; }
    public String getFunctionName()        { return functionName; }
    public String getFunctionType()        { return functionType; }
    public String getServiceUrl()          { return serviceUrl; }
    public String getServiceType()         { return serviceType; }
    public String getModelName()           { return modelName; }
    public List<String> getInputFields()   { return inputFields; }
    public List<String> getOutputFields()  { return outputFields; }
    public int getBatchSize()              { return batchSize; }
    public long getMaxWaitMs()             { return maxWaitMs; }
    public int getAsyncCapacity()          { return asyncCapacity; }
    public long getAsyncTimeoutMs()        { return asyncTimeoutMs; }
    public FailureStrategy getFailureStrategy(){ return failureStrategy; }
    public String getMcpServerUrl()        { return mcpServerUrl; }
}
