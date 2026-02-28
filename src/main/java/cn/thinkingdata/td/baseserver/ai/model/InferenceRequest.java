package cn.thinkingdata.td.baseserver.ai.model;

import cn.thinkingdata.td.baseserver.ai.config.InferenceConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 推理请求 DTO，支持 Triton（KFServing v2）和 TorchServe REST 格式序列化。
 */
public class InferenceRequest {

    private final InferenceBatch batch;
    private final InferenceConfig config;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public InferenceRequest(InferenceBatch batch, InferenceConfig config) {
        this.batch  = batch;
        this.config = config;
    }

    /**
     * 根据 serviceType 序列化为 JSON 字符串。
     */
    public String toJson() throws Exception {
        switch (config.getServiceType() == null ? "custom" : config.getServiceType()) {
            case "triton":      return toTritonJson();
            case "torchserve":  return toTorchServeJson();
            default:            return toGenericJson();
        }
    }

    /** Triton v2 格式：{"inputs":[{"name":"input","shape":[n,m],"datatype":"FP32","data":[...]}]} */
    private String toTritonJson() throws Exception {
        List<Row> rows = batch.getRows();
        List<String> inFields = config.getInputFields();
        int n = rows.size(), m = inFields.size();
        List<Object> flatData = new ArrayList<>(n * m);
        for (Row row : rows) {
            for (String field : inFields) {
                flatData.add(getFieldValue(row, field));
            }
        }
        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode inputs = root.putArray("inputs");
        ObjectNode inputTensor = inputs.addObject();
        inputTensor.put("name", "input");
        ArrayNode shape = inputTensor.putArray("shape");
        shape.add(n); shape.add(m);
        inputTensor.put("datatype", "FP32");
        ArrayNode data = inputTensor.putArray("data");
        flatData.forEach(v -> data.add(v == null ? 0.0 : Double.parseDouble(String.valueOf(v))));
        return MAPPER.writeValueAsString(root);
    }

    /** TorchServe 格式：{"instances":[{"field1":v1,"field2":v2},...]} */
    private String toTorchServeJson() throws Exception {
        List<Map<String, Object>> instances = new ArrayList<>();
        for (Row row : batch.getRows()) {
            Map<String, Object> instance = new LinkedHashMap<>();
            for (String field : config.getInputFields()) {
                instance.put(field, getFieldValue(row, field));
            }
            instances.add(instance);
        }
        return MAPPER.writeValueAsString(Map.of("instances", instances));
    }

    /** 通用格式：{"model":..., "inputs":[{"field1":v1,...},...]} */
    private String toGenericJson() throws Exception {
        List<Map<String, Object>> inputList = new ArrayList<>();
        for (Row row : batch.getRows()) {
            Map<String, Object> item = new LinkedHashMap<>();
            for (String field : config.getInputFields()) {
                item.put(field, getFieldValue(row, field));
            }
            inputList.add(item);
        }
        return MAPPER.writeValueAsString(Map.of(
                "model", config.getModelName() == null ? "" : config.getModelName(),
                "inputs", inputList));
    }

    private Object getFieldValue(Row row, String fieldName) {
        // 按字段名在 inputFields 中的位置定位（简化实现，生产中需传入字段索引 Map）
        List<String> allFields = config.getInputFields();
        int idx = allFields.indexOf(fieldName);
        return idx >= 0 ? row.getField(idx) : null;
    }
}
