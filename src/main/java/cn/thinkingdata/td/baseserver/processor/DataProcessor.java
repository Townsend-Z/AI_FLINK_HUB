package cn.thinkingdata.td.baseserver.processor;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

/**
 * 数据处理器接口，支持转换、过滤、聚合等操作。
 * 各实现按需串联，形成处理链。
 */
public interface DataProcessor {

    /**
     * 对输入流执行处理逻辑，返回处理后的流。
     *
     * @param input 上游数据流
     * @return 处理后的数据流
     */
    DataStream<Row> process(DataStream<Row> input);

    /** 处理器名称，用于日志/监控 */
    default String getName() {
        return getClass().getSimpleName();
    }
}
