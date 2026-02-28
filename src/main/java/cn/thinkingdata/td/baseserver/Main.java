package cn.thinkingdata.td.baseserver;

import cn.thinkingdata.td.baseserver.config.pipeline.PipelineConfig;
import cn.thinkingdata.td.baseserver.db.DbConfig;
import cn.thinkingdata.td.baseserver.db.DbConfigLoader;
import cn.thinkingdata.td.baseserver.pipeline.FlinkPipeline;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * 程序入口：从数据库动态加载 Pipeline 配置（AI 函数 + 普通算子），启动 Flink 流式作业。
 *
 * <p>启动命令：
 * <pre>
 * flink run flinkDemo.jar \
 *   --job-name fraud-detection-job \
 *   --db-url   jdbc:mysql://localhost:3306/flink_config \
 *   --db-user  flink \
 *   --db-password xxxxxx
 * </pre>
 *
 * <p>本地开发调试可直接运行 main 方法，修改参数默认值即可。
 */
public class Main {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String jobName    = params.get("job-name",    "fraud-detection-job");
        String dbUrl      = params.get("db-url",      "jdbc:mysql://10.82.4.191:3306/common");
        String dbUser     = params.get("db-user",     "ta");
        String dbPassword = params.get("db-password", "thinkingdata2018");

        // 从数据库加载完整 Pipeline 配置
        DbConfig dbConfig = DbConfig.of(dbUrl, dbUser, dbPassword);
        try (DbConfigLoader loader = new DbConfigLoader(dbConfig)) {
            PipelineConfig pipelineConfig = loader.load(jobName);
            // 根据配置动态生成所有算子并执行
            FlinkPipeline.fromConfig(pipelineConfig).execute();
        }
    }
}
