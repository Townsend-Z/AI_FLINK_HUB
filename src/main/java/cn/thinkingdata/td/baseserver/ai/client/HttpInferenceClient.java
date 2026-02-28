package cn.thinkingdata.td.baseserver.ai.client;

import cn.thinkingdata.td.baseserver.ai.config.InferenceConfig;
import cn.thinkingdata.td.baseserver.ai.model.InferenceBatch;
import cn.thinkingdata.td.baseserver.ai.model.InferenceRequest;
import cn.thinkingdata.td.baseserver.ai.model.InferenceResponse;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * HTTP REST 推理客户端，兼容 Triton Inference Server（KFServing v2）和 TorchServe。
 *
 * <p>设计要点：
 * <ul>
 *   <li>使用 Java 11 内置 {@link HttpClient}，单例复用（TCP 连接池）</li>
 *   <li>HttpClient 懒初始化（不参与序列化）</li>
 *   <li>失败时按 {@code maxRetries} 进行指数退避重试</li>
 * </ul>
 */
public class HttpInferenceClient implements InferenceClient {

    private static final long serialVersionUID = 1L;

    private transient HttpClient httpClient;
    private transient InferenceConfig config;

    @Override
    public void init(InferenceConfig config) {
        this.config = config;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(config.getAsyncTimeoutMs()))
                .build();
    }

    @Override
    public List<Map<String, Object>> batchInfer(InferenceBatch batch,
                                                 InferenceConfig config) throws Exception {
        ensureInitialized(config);

        InferenceRequest request = new InferenceRequest(batch, config);
        String requestBody = request.toJson();

        String responseBody = sendWithRetry(config.getServiceUrl(), requestBody, 3);
        return InferenceResponse.parse(responseBody, config, batch.size());
    }

    // -------------------------------------------------------------------------
    // Private
    // -------------------------------------------------------------------------

    private String sendWithRetry(String url, String body, int maxRetries) throws Exception {
        Exception lastException = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                return send(url, body);
            } catch (Exception e) {
                lastException = e;
                if (attempt < maxRetries) {
                    long backoffMs = (long) Math.pow(2, attempt) * 100;
                    Thread.sleep(backoffMs);
                }
            }
        }
        throw new RuntimeException("推理请求失败，已重试 " + maxRetries + " 次", lastException);
    }

    private String send(String url, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .timeout(Duration.ofMillis(config.getAsyncTimeoutMs()))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new RuntimeException("推理服务返回非 2xx 状态码: " + response.statusCode()
                    + ", body: " + response.body().substring(0, Math.min(200, response.body().length())));
        }
        return response.body();
    }

    private void ensureInitialized(InferenceConfig config) {
        if (httpClient == null) {
            init(config);
        }
    }

    @Override
    public void close() throws IOException {
        // Java 11 HttpClient 无需显式关闭
    }
}
