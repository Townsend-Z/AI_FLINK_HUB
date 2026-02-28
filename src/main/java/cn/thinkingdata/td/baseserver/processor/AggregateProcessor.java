package cn.thinkingdata.td.baseserver.processor;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

/**
 * 聚合处理器（占位实现，当前直传）。
 *
 * 后续可在此实现：
 *  - 滚动窗口 / 滑动窗口聚合（count、sum、avg 等）
 *  - Session 窗口
 *  - KeyBy + Reduce
 *  - 需要注意：聚合后 Row 的字段结构可能变化，需同步调整 SchemaDefinition
 */
public class AggregateProcessor implements DataProcessor {

    @Override
    public DataStream<Row> process(DataStream<Row> input) {
        // TODO: 实现聚合逻辑
        return input;
    }
}
