package cn.thinkingdata.td.baseserver.ai.client;

import cn.thinkingdata.td.baseserver.ai.config.InferenceConfig;
import cn.thinkingdata.td.baseserver.ai.model.InferenceBatch;
import org.apache.flink.types.Row;

import java.io.Closeable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 推理客户端接口。实现类必须：
 * <ul>
 *   <li>实现 {@link Serializable}（Flink 序列化分发到各 TaskManager）</li>
 *   <li>在 {@link #init} 中懒初始化非序列化资源（如 HttpClient）</li>
 * </ul>
 */
public interface InferenceClient extends Serializable, Closeable {

    /**
     * 初始化（在 Flink RichFunction.open() 中调用）。
     */
    void init(InferenceConfig config);

    /**
     * 批量推理。
     *
     * @param batch  输入批次
     * @param config 推理配置
     * @return 每条记录对应的推理结果 Map，顺序与 batch.getRows() 一致，长度相同
     */
    List<Map<String, Object>> batchInfer(InferenceBatch batch, InferenceConfig config) throws Exception;
}
