package cn.thinkingdata.td.baseserver.config.pipeline;

/**
 * 单个处理器配置，对应 td_pipeline_processor 表的一行。
 */
public class ProcessorSpec {

    /** "filter" | "transform" | "aggregate" | "ai-inference" */
    private String type;
    /** processor_config 字段（JSON 字符串） */
    private String processorConfig;
    /** ai-inference 时关联的 AiFunctionSpec */
    private AiFunctionSpec aiFunctionSpec;

    public ProcessorSpec() {}

    public String getType()                       { return type; }
    public void setType(String v)                 { this.type = v; }
    public String getProcessorConfig()            { return processorConfig; }
    public void setProcessorConfig(String v)      { this.processorConfig = v; }
    public AiFunctionSpec getAiFunctionSpec()      { return aiFunctionSpec; }
    public void setAiFunctionSpec(AiFunctionSpec v){ this.aiFunctionSpec = v; }
}
