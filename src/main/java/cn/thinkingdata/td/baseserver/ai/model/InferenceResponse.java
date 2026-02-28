package cn.thinkingdata.td.baseserver.ai.model;

import cn.thinkingdata.td.baseserver.ai.config.InferenceConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 推理响应 DTO，将推理服务返回的 JSON 解析为
 * {@code List<Map<String, Object>>}（每条记录对应一个 Map，key 为 outputField 名）。
 */
public class InferenceResponse {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * 解析推理服务响应 JSON。
     *
     * @param responseJson 推理服务返回的 JSON 字符串
     * @param config       推理配置（含 outputFields、serviceType）
     * @param batchSize    批次大小（用于校验结果条数）
     * @return 每条记录对应的结果 Map 列表
     */
    public static List<Map<String, Object>> parse(String responseJson,
                                                   InferenceConfig config,
                                                   int batchSize) throws Exception {
        List<String> outFields = config.getOutputFields();
        switch (config.getServiceType() == null ? "custom" : config.getServiceType()) {
            case "triton":      return parseTriton(responseJson, outFields, batchSize);
            case "torchserve":  return parseTorchServe(responseJson, outFields);
            default:            return parseGeneric(responseJson, outFields);
        }
    }

    /** Triton 响应：{"outputs":[{"name":"output","shape":[n,m],"data":[...]}]} */
    private static List<Map<String, Object>> parseTriton(String json,
                                                           List<String> outFields,
                                                           int batchSize) throws Exception {
        JsonNode root = MAPPER.readTree(json);
        JsonNode outputsNode = root.get("outputs");
        List<Object> flatData = new ArrayList<>();
        if (outputsNode != null && outputsNode.isArray()) {
            outputsNode.get(0).get("data").forEach(n -> flatData.add(n.asDouble()));
        }
        List<Map<String, Object>> results = new ArrayList<>();
        int m = outFields.size();
        for (int i = 0; i < batchSize; i++) {
            Map<String, Object> record = new LinkedHashMap<>();
            for (int j = 0; j < m; j++) {
                int idx = i * m + j;
                record.put(outFields.get(j), idx < flatData.size() ? flatData.get(idx) : null);
            }
            results.add(record);
        }
        return results;
    }

    /** TorchServe 响应：{"predictions":[{"pred":..., "score":...},...]} */
    private static List<Map<String, Object>> parseTorchServe(String json,
                                                              List<String> outFields) throws Exception {
        JsonNode root = MAPPER.readTree(json);
        JsonNode preds = root.has("predictions") ? root.get("predictions") : root;
        List<Map<String, Object>> results = new ArrayList<>();
        if (preds.isArray()) {
            for (JsonNode pred : preds) {
                Map<String, Object> record = new LinkedHashMap<>();
                for (String field : outFields) {
                    record.put(field, pred.has(field) ? pred.get(field).asText() : null);
                }
                results.add(record);
            }
        }
        return results;
    }

    /** 通用响应：{"results":[{"field1":v1,...},...]} */
    private static List<Map<String, Object>> parseGeneric(String json,
                                                           List<String> outFields) throws Exception {
        JsonNode root = MAPPER.readTree(json);
        JsonNode results = root.has("results") ? root.get("results") : root;
        List<Map<String, Object>> list = new ArrayList<>();
        if (results.isArray()) {
            for (JsonNode item : results) {
                Map<String, Object> record = new LinkedHashMap<>();
                for (String field : outFields) {
                    record.put(field, item.has(field) ? item.get(field).asText() : null);
                }
                list.add(record);
            }
        }
        return list;
    }
}
