package cn.thinkingdata.td.baseserver.ai.processor;

import cn.thinkingdata.td.baseserver.ai.client.InferenceClient;
import cn.thinkingdata.td.baseserver.ai.config.InferenceConfig;
import cn.thinkingdata.td.baseserver.ai.model.InferenceBatch;
import cn.thinkingdata.td.baseserver.processor.DataProcessor;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

/**
 * AI 推理 DataProcessor 实现，组装双阶段流水线：
 *
 * <pre>
 * DataStream&lt;Row&gt;
 *   → BatchCollectorFunction（Stage 1：攒批）
 *   → AsyncDataStream.unorderedWait（Stage 2：异步推理）
 *   → DataStream&lt;Row&gt;（带推理结果字段）
 * </pre>
 */
public class AiInferenceProcessor implements DataProcessor {

    private final InferenceConfig config;
    private final InferenceClient inferenceClient;

    public AiInferenceProcessor(InferenceConfig config, InferenceClient inferenceClient) {
        this.config          = config;
        this.inferenceClient = inferenceClient;
    }

    @Override
    public DataStream<Row> process(DataStream<Row> input) {
        // Stage 1: 攒批
        DataStream<InferenceBatch> batchStream = input
                .process(new BatchCollectorFunction(config))
                .name("ai-batch-collector[" + config.getFunctionName() + "]");

        // Stage 2: 异步推理
        // unorderedWait：不等待批次结果有序化，吞吐优先
        return AsyncDataStream.unorderedWait(
                batchStream,
                new AsyncInferenceFunction(config, inferenceClient),
                config.getAsyncTimeoutMs(),
                TimeUnit.MILLISECONDS,
                config.getAsyncCapacity()
        ).name("ai-async-inference[" + config.getFunctionName() + "]");
    }

    @Override
    public String getName() {
        return "AiInferenceProcessor[" + config.getFunctionName() + "]";
    }
}
