package cn.thinkingdata.td.baseserver.ai.model;

import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

/**
 * 一个推理批次的包装，包含批次 ID（用于日志追踪）和一组待推理的 Row。
 */
public class InferenceBatch implements Serializable {

    private final String batchId;
    private final List<Row> rows;
    private final long createTimeMs;

    public InferenceBatch(List<Row> rows) {
        this.batchId      = UUID.randomUUID().toString();
        this.rows         = rows;
        this.createTimeMs = System.currentTimeMillis();
    }

    public String getBatchId()     { return batchId; }
    public List<Row> getRows()     { return rows; }
    public long getCreateTimeMs()  { return createTimeMs; }
    public int size()              { return rows.size(); }
}
