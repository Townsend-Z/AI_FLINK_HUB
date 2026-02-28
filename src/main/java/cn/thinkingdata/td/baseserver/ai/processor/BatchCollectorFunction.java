package cn.thinkingdata.td.baseserver.ai.processor;

import cn.thinkingdata.td.baseserver.ai.config.InferenceConfig;
import cn.thinkingdata.td.baseserver.ai.model.InferenceBatch;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Stage 1：攒批 ProcessFunction。
 *
 * <p>将上游 {@link Row} 流按条数和超时双触发策略聚合为 {@link InferenceBatch}，
 * 发往 Stage 2 的 {@link AsyncInferenceFunction} 进行异步推理。
 *
 * <p>触发策略：
 * <ul>
 *   <li>条数触发：buffer 大小达到 {@code maxBatchSize} 时立即 flush</li>
 *   <li>超时触发：第一条数据到达后注册处理时间 Timer，超时 {@code maxWaitMs} 后 flush</li>
 * </ul>
 *
 * <p>容错：buffer 使用 {@link ListState} 持久化，Checkpoint 后可恢复。
 */
public class BatchCollectorFunction
        extends ProcessFunction<Row, InferenceBatch>
        implements CheckpointedFunction {

    private final InferenceConfig config;

    // Flink Operator State（持久化，支持 Checkpoint 恢复）
    private ListState<Row> bufferState;
    // 本地 buffer（加速，避免每次读取 state）
    private transient List<Row> localBuffer;
    private transient boolean timerRegistered;

    public BatchCollectorFunction(InferenceConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
        localBuffer    = new ArrayList<>();
        timerRegistered = false;
    }

    @Override
    public void processElement(Row row, Context ctx, Collector<InferenceBatch> out) throws Exception {
        localBuffer.add(row);

        // 第一条数据到达时注册超时 Timer
        if (localBuffer.size() == 1 && !timerRegistered) {
            ctx.timerService().registerProcessingTimeTimer(
                    ctx.timerService().currentProcessingTime() + config.getMaxWaitMs());
            timerRegistered = true;
        }

        // 条数达到阈值：立即 flush
        if (localBuffer.size() >= config.getBatchSize()) {
            flush(out, ctx);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<InferenceBatch> out) throws Exception {
        // 超时 flush（保障低延迟）
        if (!localBuffer.isEmpty()) {
            flush(out, ctx);
        }
    }

    // -------------------------------------------------------------------------
    // CheckpointedFunction（Operator State）
    // -------------------------------------------------------------------------

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        bufferState.clear();
        for (Row row : localBuffer) {
            bufferState.add(row);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Row> descriptor = new ListStateDescriptor<>(
                "batch-buffer",
                Types.ROW_NAMED(new String[]{}, new org.apache.flink.api.common.typeinfo.TypeInformation[0]));
        bufferState = context.getOperatorStateStore().getListState(descriptor);

        if (localBuffer == null) localBuffer = new ArrayList<>();

        // 从 Checkpoint 恢复
        if (context.isRestored()) {
            for (Row row : bufferState.get()) {
                localBuffer.add(row);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Private
    // -------------------------------------------------------------------------

    private void flush(Collector<InferenceBatch> out, Object ctx) {
        if (localBuffer.isEmpty()) return;
        out.collect(new InferenceBatch(new ArrayList<>(localBuffer)));
        localBuffer.clear();
        timerRegistered = false;
    }
}
