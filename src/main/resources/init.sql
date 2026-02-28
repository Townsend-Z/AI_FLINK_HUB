-- =============================================================
-- Flink 动态配置数据库初始化脚本
-- 数据库：MySQL 5.7+ / PostgreSQL 12+（注意 ON UPDATE 语法差异）
-- =============================================================

-- ---------------------------------------------------------------
-- 1. AI 推理函数定义注册表
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS td_ai_function (
    function_id       VARCHAR(64)   NOT NULL PRIMARY KEY       COMMENT 'AI函数唯一标识',
    function_name     VARCHAR(128)  NOT NULL                   COMMENT 'AI函数可读名称',
    function_type     VARCHAR(32)   NOT NULL                   COMMENT '内置类型: http | mcp',
    service_url       VARCHAR(512)                             COMMENT '推理服务地址',
    service_type      VARCHAR(32)                              COMMENT 'triton | torchserve | custom',
    model_name        VARCHAR(128)                             COMMENT '模型名称',
    input_fields      VARCHAR(1024)                            COMMENT '输入字段名，JSON数组，如 ["amount","user_id"]',
    output_fields     VARCHAR(1024)                            COMMENT '输出字段名，JSON数组，如 ["prediction","score"]',
    batch_size        INT           DEFAULT 64                 COMMENT '批量推理最大条数',
    max_wait_ms       INT           DEFAULT 100                COMMENT '攒批最长等待时间(ms)',
    async_capacity    INT           DEFAULT 100                COMMENT 'in-flight最大并发批次',
    failure_strategy  VARCHAR(16)   DEFAULT 'PASS_THROUGH'     COMMENT 'SKIP|PASS_THROUGH|FAIL',
    extra_config      TEXT                                     COMMENT '附加配置(JSON)，如MCP工具列表、HTTP请求头等',
    status            TINYINT       DEFAULT 1                  COMMENT '1=启用 0=禁用',
    created_at        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) COMMENT 'AI推理函数定义注册表';

-- ---------------------------------------------------------------
-- 2. Flink 作业处理链配置表
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS td_pipeline_processor (
    id              BIGINT        NOT NULL AUTO_INCREMENT PRIMARY KEY,
    job_name        VARCHAR(128)  NOT NULL                   COMMENT '关联的Flink作业名',
    step_order      INT           NOT NULL                   COMMENT '执行顺序，从1开始',
    processor_type  VARCHAR(32)   NOT NULL                   COMMENT 'filter|transform|aggregate|ai-inference',
    function_id     VARCHAR(64)                              COMMENT 'processor_type=ai-inference时关联td_ai_function',
    processor_config TEXT                                    COMMENT '处理器附加配置(JSON)，filter/transform/aggregate时使用',
    status          TINYINT       DEFAULT 1,
    UNIQUE KEY uk_job_order (job_name, step_order)
) COMMENT 'Flink作业处理链配置';

-- ---------------------------------------------------------------
-- 3. Flink 作业配置主表（Kafka/Paimon/Schema/并行度）
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS td_flink_job (
    job_name       VARCHAR(128) NOT NULL PRIMARY KEY,
    parallelism    INT          DEFAULT 1,
    kafka_config   TEXT         NOT NULL   COMMENT 'Kafka连接配置(JSON)',
    paimon_config  TEXT         NOT NULL   COMMENT 'Paimon配置(JSON)',
    schema_config  TEXT         NOT NULL   COMMENT '目标表Schema定义(JSON)',
    status         TINYINT      DEFAULT 1,
    created_at     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) COMMENT 'Flink作业配置主表';


-- =============================================================
-- 示例数据：欺诈检测作业 fraud-detection-job
-- =============================================================

-- 1) AI 函数：Triton HTTP 推理
INSERT INTO td_ai_function
    (function_id, function_name, function_type, service_url, service_type, model_name,
     input_fields, output_fields, batch_size, max_wait_ms, async_capacity, failure_strategy, status)
VALUES (
    'fraud-detect-v1',
    '欺诈检测模型 v1',
    'http',
    'http://triton:8000/v2/models/fraud/infer',
    'triton',
    'fraud_v1',
    '["user_id","amount","merchant"]',
    '["is_fraud","fraud_score"]',
    64, 100, 100, 'PASS_THROUGH', 1
);

-- 2) 作业主配置
INSERT INTO td_flink_job (job_name, parallelism, kafka_config, paimon_config, schema_config)
VALUES (
    'fraud-detection-job',
    2,
    -- kafka_config JSON
    '{
        "bootstrapServers": "kafka:9092",
        "topic": "transactions",
        "groupId": "fraud-flink-consumer",
        "startupMode": "LATEST"
    }',
    -- paimon_config JSON
    '{
        "warehouse": "/data/paimon/warehouse",
        "catalogName": "paimon_catalog",
        "database": "ods",
        "tableProperties": {
            "changelog-producer": "input",
            "bucket": "8"
        }
    }',
    -- schema_config JSON（Kafka 消费字段 + AI 推理输出字段）
    '{
        "table": "transactions_enriched",
        "fields": [
            {"name": "user_id",     "type": "LONG",      "nullable": false},
            {"name": "amount",      "type": "DOUBLE"},
            {"name": "merchant",    "type": "STRING"},
            {"name": "event_time",  "type": "TIMESTAMP"},
            {"name": "is_fraud",    "type": "BOOLEAN"},
            {"name": "fraud_score", "type": "DOUBLE"}
        ],
        "primaryKeys": ["user_id", "event_time"]
    }'
);

-- 3) 处理链：filter → transform → ai-inference
INSERT INTO td_pipeline_processor (job_name, step_order, processor_type, function_id, processor_config) VALUES
-- Step 1: 过滤金额为0或负数的交易
(
    'fraud-detection-job', 1, 'filter', NULL,
    '{"logic":"AND","conditions":[{"field":"amount","op":"gt","value":0}]}'
),
-- Step 2: 字段标准化（merchant 转大写）
(
    'fraud-detection-job', 2, 'transform', NULL,
    '{"mappings":[{"output":"merchant","input":"merchant","op":"upper"}]}'
),
-- Step 3: AI 欺诈推理
(
    'fraud-detection-job', 3, 'ai-inference', 'fraud-detect-v1', NULL
);
