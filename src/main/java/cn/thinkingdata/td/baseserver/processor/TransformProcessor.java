package cn.thinkingdata.td.baseserver.processor;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

/**
 * 字段转换处理器（占位实现，当前直传）。
 *
 * 后续可在此实现：
 *  - 字段重命名
 *  - 类型转换
 *  - 计算派生字段（如 event_time = parse(raw_time)）
 *  - 字段脱敏
 */
public class TransformProcessor implements DataProcessor {

    @Override
    public DataStream<Row> process(DataStream<Row> input) {
        // TODO: 实现字段转换逻辑
        return input;
    }
}
