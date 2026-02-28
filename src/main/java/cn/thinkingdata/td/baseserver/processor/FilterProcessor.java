package cn.thinkingdata.td.baseserver.processor;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

/**
 * 数据过滤处理器（占位实现，当前直传）。
 *
 * 后续可在此实现：
 *  - 按字段值过滤（如过滤无效事件类型）
 *  - 去重
 *  - 异常数据剔除
 */
public class FilterProcessor implements DataProcessor {

    @Override
    public DataStream<Row> process(DataStream<Row> input) {
        // TODO: 实现过滤逻辑，例如：
        // return input.filter(row -> row.getField(0) != null);
        return input;
    }
}
