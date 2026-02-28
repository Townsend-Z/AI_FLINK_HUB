package cn.thinkingdata.td.baseserver.ai.config;

/**
 * 推理失败处理策略
 */
public enum FailureStrategy {
    /** 跳过该条/该批次数据，不下发 */
    SKIP,
    /** 原样透传（不填充推理结果字段，保持 null），继续下发 */
    PASS_THROUGH,
    /** 抛出异常，让 Flink 任务感知失败 */
    FAIL
}
