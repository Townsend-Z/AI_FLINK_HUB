package cn.thinkingdata.td.baseserver.ai.processor;

import cn.thinkingdata.td.baseserver.ai.client.InferenceClient;
import cn.thinkingdata.td.baseserver.ai.config.FailureStrategy;
import cn.thinkingdata.td.baseserver.ai.config.InferenceConfig;
import cn.thinkingdata.td.baseserver.ai.model.InferenceBatch;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Stage 2：异步推理 RichAsyncFunction。
 *
 * <p>接收 {@link InferenceBatch}，通过独立 IO 线程池异步调用 {@link InferenceClient}，
 * 将推理结果合并回每条 {@link Row} 后输出。
 *
 * <p>多个批次可同时 in-flight（由 {@code AsyncDataStream} 的 {@code capacity} 参数控制）。
 */
public class AsyncInferenceFunction extends RichAsyncFunction<InferenceBatch, Row> {

    private final InferenceConfig config;
    private final InferenceClient inferenceClient;

    private transient ExecutorService ioExecutor;

    public AsyncInferenceFunction(InferenceConfig config, InferenceClient inferenceClient) {
        this.config          = config;
        this.inferenceClient = inferenceClient;
    }

    @Override
    public void open(Configuration parameters) {
        inferenceClient.init(config);
        // 独立 IO 线程池，不占用 Flink 算子线程
        ioExecutor = Executors.newCachedThreadPool();
    }

    @Override
    public void close() throws Exception {
        if (ioExecutor != null) ioExecutor.shutdownNow();
        if (inferenceClient != null) inferenceClient.close();
    }

    @Override
    public void asyncInvoke(InferenceBatch batch, ResultFuture<Row> resultFuture) {
        ioExecutor.submit(() -> {
            try {
                List<Map<String, Object>> results = inferenceClient.batchInfer(batch, config);
                List<Row> enrichedRows = mergeResults(batch.getRows(), results);
                resultFuture.complete(enrichedRows);
            } catch (Exception e) {
                handleFailure(e, batch, resultFuture);
            }
        });
    }

    @Override
    public void timeout(InferenceBatch batch, ResultFuture<Row> resultFuture) {
        handleFailure(new RuntimeException("推理超时（>" + config.getAsyncTimeoutMs() + "ms）"),
                batch, resultFuture);
    }

    // -------------------------------------------------------------------------
    // Private
    // -------------------------------------------------------------------------

    /**
     * 将推理结果 Map 合并回原始 Row，按 outputFields 追加字段。
     * 输出 Row 的字段顺序 = 原始 Row 字段 + outputFields（已在 SchemaDefinition 中定义）。
     */
    private List<Row> mergeResults(List<Row> inputRows, List<Map<String, Object>> results) {
        List<Row> enriched = new ArrayList<>(inputRows.size());
        List<String> outFields = config.getOutputFields();
        int inArity    = inputRows.isEmpty() ? 0 : inputRows.get(0).getArity();
        int totalArity = inArity + outFields.size();

        for (int i = 0; i < inputRows.size(); i++) {
            Row inRow  = inputRows.get(i);
            Row outRow = new Row(totalArity);

            // 复制原始字段
            for (int j = 0; j < inArity; j++) {
                outRow.setField(j, inRow.getField(j));
            }
            // 填充推理结果字段
            Map<String, Object> result = i < results.size() ? results.get(i) : Collections.emptyMap();
            for (int k = 0; k < outFields.size(); k++) {
                outRow.setField(inArity + k, result.get(outFields.get(k)));
            }
            enriched.add(outRow);
        }
        return enriched;
    }

    private void handleFailure(Exception e, InferenceBatch batch, ResultFuture<Row> resultFuture) {
        FailureStrategy strategy = config.getFailureStrategy();
        switch (strategy) {
            case SKIP:
                // 跳过该批次，不输出任何 Row
                resultFuture.complete(Collections.emptyList());
                break;
            case PASS_THROUGH:
                // 原样透传，推理结果字段填 null
                resultFuture.complete(mergeResults(batch.getRows(), Collections.emptyList()));
                break;
            case FAIL:
            default:
                resultFuture.completeExceptionally(e);
        }
    }
}
