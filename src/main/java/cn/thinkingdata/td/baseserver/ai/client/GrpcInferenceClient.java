package cn.thinkingdata.td.baseserver.ai.client;

import cn.thinkingdata.td.baseserver.ai.config.InferenceConfig;
import cn.thinkingdata.td.baseserver.ai.model.InferenceBatch;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * gRPC 推理客户端（占位实现）。
 *
 * <p>使用时需添加依赖：
 * <pre>
 * &lt;dependency&gt;
 *   &lt;groupId&gt;io.grpc&lt;/groupId&gt;
 *   &lt;artifactId&gt;grpc-netty-shaded&lt;/artifactId&gt;
 *   &lt;version&gt;1.60.0&lt;/version&gt;
 * &lt;/dependency&gt;
 * </pre>
 * 并根据具体推理服务的 proto 生成 Stub 代码后实现 {@link #batchInfer} 方法。
 */
public class GrpcInferenceClient implements InferenceClient {

    private static final long serialVersionUID = 1L;

    @Override
    public void init(InferenceConfig config) {
        // TODO: 初始化 gRPC channel 和 stub
        throw new UnsupportedOperationException("gRPC 推理客户端尚未实现，请添加 gRPC 依赖并实现此方法");
    }

    @Override
    public List<Map<String, Object>> batchInfer(InferenceBatch batch,
                                                 InferenceConfig config) throws Exception {
        // TODO: 实现 gRPC 调用
        throw new UnsupportedOperationException("gRPC 推理客户端尚未实现");
    }

    @Override
    public void close() throws IOException {
        // TODO: 关闭 gRPC channel
    }
}
