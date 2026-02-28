# Flink 动态流处理框架 (FlinkDemo)

> 🤖 本项目由 **GitHub Copilot CLI AI** 全程辅助规划、编码与调试构建

---

## 项目简介

**FlinkDemo** 是一个基于 Apache Flink 1.19 的**动态流处理框架**，实现从 Kafka 消费数据、经过可配置的数据处理链路（过滤 / 转换 / 聚合 / AI 推理），最终写入 Apache Paimon 数据湖表。

所有处理逻辑均由 **MySQL 数据库驱动**，无需修改代码或重新编译 JAR 即可动态调整 Pipeline 行为，适用于需要快速迭代数据处理规则的场景。

---

## 核心特性

| 特性 | 描述 |
|------|------|
| 📡 **Kafka 消费** | 支持自定义 Schema 反序列化，按字段类型解析 JSON 消息 |
| 🗃️ **Paimon 写入** | 基于 Flink Table API 写入 Apache Paimon 表，支持本地文件系统与 HDFS |
| 🔧 **动态配置** | Pipeline 全部配置（Source / Sink / Schema / 处理规则）存储于 MySQL，运行时加载 |
| 🔍 **数据过滤** | 基于规则引擎的多条件过滤（AND/OR），支持 eq/gt/lt/contains/regex 等操作符 |
| 🔄 **数据转换** | 字段映射、类型转换、表达式计算 |
| 📊 **数据聚合** | 基于滑动/滚动/会话窗口的 Group-By 聚合 |
| 🤖 **AI 推理** | 内置 HTTP / gRPC / MCP 三种协议的 AI 推理算子，支持批量推理与异步处理 |
| 🔗 **MCP 协议** | 支持 Model Context Protocol，可对接外部工具（DB 查询、API 调用）增强 AI 推理 |

---

## 架构总览

```
┌──────────────────────────────────────────────────────────────────┐
│                        MySQL (flink_config)                       │
│  td_flink_job  │  td_pipeline_processor  │  td_ai_function        │
└──────────────────────┬───────────────────────────────────────────┘
                       │ 运行时加载配置
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│                        FlinkPipeline                              │
│                                                                   │
│  Kafka Source  →  SchemaBasedDeserializer  →  DataStream<Row>     │
│                                                  │                │
│                              ┌───────────────────┤               │
│                              ▼                   ▼               │
│                     ConfigurableFilter    ConfigurableTransform   │
│                              │                   │               │
│                              └─────────┬─────────┘               │
│                                        ▼                         │
│                              ConfigurableAggregate                │
│                                        │                         │
│                                        ▼                         │
│                              AiInferenceProcessor                 │
│                         (Batch → HTTP/gRPC/MCP)                   │
│                                        │                         │
│                                        ▼                         │
│                             Paimon Table Sink                     │
└──────────────────────────────────────────────────────────────────┘
```

---

## 模块说明

### 核心模块

| 模块 | 包路径 | 说明 |
|------|--------|------|
| **入口** | `Main.java` | 从 MySQL 加载配置，创建并执行 FlinkPipeline |
| **Pipeline 编排** | `pipeline.FlinkPipeline` | 组装 Kafka Source → 处理链 → Paimon Sink |
| **配置加载** | `db.DbConfigLoader` | 查询 MySQL 三张配置表，构建 PipelineConfig 对象 |
| **Schema 反序列化** | `serialization.SchemaBasedDeserializer` | 按 SchemaDefinition 将 Kafka JSON 解析为 Flink Row |

### 数据处理算子

| 算子 | 类 | 说明 |
|------|----|------|
| **过滤** | `ConfigurableFilterProcessor` | 支持多条件 AND/OR 组合，动态规则 |
| **转换** | `ConfigurableTransformProcessor` | 字段重命名、类型转换、表达式映射 |
| **聚合** | `ConfigurableAggregateProcessor` | 窗口聚合，支持 sum/avg/count/max/min |
| **AI 推理** | `AiInferenceProcessor` | 批量采集 + 异步调用推理服务 |

### AI 推理模块

| 组件 | 说明 |
|------|------|
| `HttpInferenceClient` | 对接 Triton / TorchServe 等 HTTP 推理服务 |
| `GrpcInferenceClient` | gRPC 协议推理客户端 |
| `McpAgentClient` | Model Context Protocol 协议，支持工具调用（DB 查询、外部 API） |
| `AiFunctionFactory` | 根据 DB 配置动态创建对应推理客户端 |
| `BatchCollectorFunction` | 将流数据攒批，提升推理吞吐 |
| `AsyncInferenceFunction` | 异步 I/O 调用推理服务，避免阻塞流处理 |

---

## 数据库配置表结构

项目由以下 3 张 MySQL 表驱动：

### `td_flink_job` — 作业主配置

```sql
CREATE TABLE td_flink_job (
    id          BIGINT PRIMARY KEY AUTO_INCREMENT,
    job_name    VARCHAR(100) NOT NULL UNIQUE,   -- 作业名称（启动参数指定）
    kafka_bootstrap VARCHAR(500),               -- Kafka 地址
    kafka_topic     VARCHAR(200),               -- 消费 Topic
    kafka_group_id  VARCHAR(200),               -- Consumer Group
    paimon_warehouse VARCHAR(500),              -- Paimon 仓库路径
    paimon_database  VARCHAR(200),              -- 目标库
    paimon_table     VARCHAR(200),              -- 目标表
    schema_json      TEXT,                      -- 字段 Schema 定义 (JSON)
    parallelism      INT DEFAULT 1
);
```

### `td_pipeline_processor` — 处理算子链

```sql
CREATE TABLE td_pipeline_processor (
    id           BIGINT PRIMARY KEY AUTO_INCREMENT,
    job_name     VARCHAR(100) NOT NULL,
    processor_order INT,                        -- 执行顺序
    processor_type VARCHAR(50),                 -- filter/transform/aggregate/ai-inference
    processor_config TEXT,                      -- 规则 JSON
    enabled      TINYINT DEFAULT 1
);
```

### `td_ai_function` — AI 推理函数配置

```sql
CREATE TABLE td_ai_function (
    id               BIGINT PRIMARY KEY AUTO_INCREMENT,
    function_name    VARCHAR(100) UNIQUE,
    inference_protocol VARCHAR(20),            -- http/grpc/mcp
    endpoint_url     VARCHAR(500),
    model_name       VARCHAR(200),
    input_fields     TEXT,                     -- 输入字段映射 (JSON)
    output_fields    TEXT,                     -- 输出字段映射 (JSON)
    batch_size       INT DEFAULT 32,
    timeout_ms       INT DEFAULT 5000,
    failure_strategy VARCHAR(20) DEFAULT 'SKIP' -- SKIP/PASS_THROUGH/FAIL
);
```

---

## 技术栈

| 组件 | 版本 |
|------|------|
| Apache Flink | 1.19.1 |
| Apache Paimon | 1.0.0 (flink-1.19) |
| Apache Kafka Connector | 3.2.0-1.19 |
| Apache Hadoop | 3.4.1 |
| Java | 11+ (推荐 Java 17) |
| MySQL | 8.x |
| HikariCP 连接池 | 5.0.1 |
| Jackson JSON | 2.15.2 |
| Maven | 3.8+ |

---

## 快速开始

### 1. 前置依赖

- Java 11+（推荐 Java 17）
- Maven 3.8+
- MySQL 8.x（运行中）
- Apache Kafka（可选，本地测试时可配置空 Topic）

### 2. 初始化数据库

```bash
# 登录 MySQL，创建数据库和用户
mysql -u root -p

CREATE DATABASE flink_config CHARACTER SET utf8mb4;
CREATE USER 'flink'@'localhost' IDENTIFIED BY 'flink123';
GRANT ALL PRIVILEGES ON flink_config.* TO 'flink'@'localhost';
FLUSH PRIVILEGES;

# 执行初始化 SQL（建表 + 示例配置）
mysql -u flink -pflink123 flink_config < src/main/resources/init.sql
```

### 3. 配置示例 Pipeline

init.sql 已内置 **欺诈检测作业（fraud-detection-job）** 示例，包含：
- Kafka Source（Topic: `user_events`，字段：user_id / event_type / amount / merchant / event_time）
- 过滤算子：`amount > 0`
- 转换算子：`merchant` 字段转大写
- AI 推理算子：批量调用 Triton HTTP 服务进行欺诈评分
- Paimon Sink：写入 `default.fraud_events` 表

### 4. 编译打包

```bash
cd flinkDemo
mvn clean package -DskipTests
```

### 5. 本地运行（开发测试）

> ⚠️ 本地测试使用 `mvn exec:java`，可保留 Flink 类加载器边界，避免 fat JAR 类冲突。

```bash
mvn exec:java \
  -Dexec.mainClass="cn.thinkingdata.td.baseserver.Main" \
  -Dexec.args="--job-name fraud-detection-job \
               --db-url jdbc:mysql://127.0.0.1:3306/flink_config \
               --db-user flink \
               --db-password flink123"
```

### 6. Flink 集群部署

```bash
# 将 fat JAR 提交到 Flink 集群
flink run \
  -p 4 \
  -c cn.thinkingdata.td.baseserver.Main \
  target/flinkDemo-1.0-SNAPSHOT.jar \
  --job-name fraud-detection-job \
  --db-url jdbc:mysql://<mysql-host>:3306/flink_config \
  --db-user flink \
  --db-password flink123
```

---

## 添加新 Pipeline

无需修改代码，直接向 MySQL 写入配置即可添加新作业：

```sql
-- 1. 新增作业配置
INSERT INTO td_flink_job (job_name, kafka_bootstrap, kafka_topic, kafka_group_id,
    paimon_warehouse, paimon_database, paimon_table, schema_json, parallelism)
VALUES ('my-new-job',
    'kafka:9092', 'my-topic', 'my-group',
    '/data/paimon/warehouse', 'default', 'my_table',
    '[{"name":"id","type":"STRING","nullable":false,"primaryKey":true},
      {"name":"value","type":"DOUBLE","nullable":true}]',
    2);

-- 2. 添加过滤算子
INSERT INTO td_pipeline_processor (job_name, processor_order, processor_type, processor_config)
VALUES ('my-new-job', 1, 'filter',
    '{"logic":"AND","conditions":[{"field":"value","operator":"gt","value":"0"}]}');

-- 3. 启动作业（无需重新编译）
-- mvn exec:java ... --job-name my-new-job
```

---

## 项目结构

```
flinkDemo/
├── src/main/java/cn/thinkingdata/td/baseserver/
│   ├── Main.java                          # 程序入口
│   ├── config/                            # 配置 POJO（Job / Kafka / Paimon / Pipeline）
│   ├── db/                                # MySQL 配置加载（DbConfigLoader）
│   ├── schema/                            # Schema 定义（字段类型、Schema 对象）
│   ├── serialization/                     # Kafka JSON 反序列化
│   ├── pipeline/                          # FlinkPipeline 核心编排类
│   ├── operator/                          # 数据处理算子
│   │   ├── ConfigurableFilterProcessor
│   │   ├── ConfigurableTransformProcessor
│   │   ├── ConfigurableAggregateProcessor
│   │   ├── rule/                          # 规则 POJO
│   │   └── engine/                        # 规则执行引擎
│   ├── processor/                         # DataProcessor 接口定义
│   └── ai/                                # AI 推理模块
│       ├── client/                        # HTTP / gRPC 推理客户端
│       ├── mcp/                           # MCP 协议客户端与工具
│       ├── processor/                     # AI 推理 Flink 算子
│       ├── model/                         # 推理请求/响应模型
│       ├── config/                        # 推理配置
│       └── factory/                       # 客户端工厂
├── src/main/resources/
│   └── init.sql                           # 数据库初始化脚本
└── pom.xml
```

---

## 注意事项

- **生产集群部署**：Flink 核心依赖（flink-streaming-java、flink-table-planner-loader 等）应设为 `<scope>provided</scope>`，由集群提供，减小 JAR 体积。
- **Paimon 本地测试**：Warehouse 路径需为可写的本地目录（如 `/tmp/paimon/warehouse`）。
- **AI 推理服务**：本地测试时需确保推理服务端点可达；Failure Strategy 可配置为 `SKIP`（跳过推理失败的记录）避免作业因推理服务不可用而终止。
- **Kafka 连接**：本地无 Kafka 时，作业会在 Kafka 连接阶段挂起，属正常现象，可通过启动本地 Kafka 或使用 Mock Source 进行端到端测试。

---

## 开发路线图

- [ ] 支持 DataGen / FileSystem Source（本地端到端测试）
- [ ] 规则热更新（不重启作业动态加载新规则）
- [ ] Web 管理界面（可视化配置 Pipeline）
- [ ] 更多 AI 推理协议（OpenAI API 兼容接口）
- [ ] 内置监控指标（Prometheus + Grafana）

---

## License

Apache License 2.0

---

<div align="center">

**🤖 Built & Debugged with [GitHub Copilot CLI](https://githubnext.com/projects/copilot-cli) AI**

*本项目从架构设计、代码实现到运行时问题排查，全程由 GitHub Copilot CLI AI 辅助完成*

</div>
