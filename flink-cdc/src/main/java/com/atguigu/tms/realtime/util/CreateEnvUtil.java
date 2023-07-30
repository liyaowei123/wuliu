package com.atguigu.tms.realtime.util;

import com.esotericsoftware.minlog.Log;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.util.HashMap;

public class CreateEnvUtil {

    /**
     * 初始化流处理环境
     *
     * @param args 命令行参数数组
     * @return 流处理环境
     */
    public static StreamExecutionEnvironment getStreamEnv(String[] args) {
        // TODO 1. 初始化流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();


        return env;
    }

    /**
     * 生成 Flink-CDC 的 MysqlSource 对象
     * @param option 选项，dim|dwd，对应不同的原始表列表
     * @param serverId  MySQL 从机的 serverId
     * @param args 命令行参数数组
     * @return MySqlSource 对象
     */
    public static MySqlSource<String> getJSONSchemaMysqlSource(String option, String serverId, String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String mysqlHostname = parameterTool.get("mysql-hostname", "master");
        int mysqlPort = Integer.parseInt(parameterTool.get("mysql-port", "3306"));
        String mysqlUsername = parameterTool.get("mysql-username", "root");
        String mysqlPasswd = parameterTool.get("mysql-passwd", "000000");
        serverId = parameterTool.get("server-id", serverId);
        option = parameterTool.get("start-up-options", option);

        // 将 Decimal 类型数据的解析格式由 BASE64 更改为 NUMERIC，否则解析报错
        // 创建配置信息 Map 集合，将 Decimal 数据类型的解析格式配置 k-v 置于其中
        HashMap config = new HashMap<>();
        config.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        // 将前述 Map 集合中的配置信息传递给 JSON 解析 Schema，该 Schema 将用于 MysqlSource 的初始化
        JsonDebeziumDeserializationSchema jsonDebeziumDeserializationSchema =
                new JsonDebeziumDeserializationSchema(false, config);

        // 创建 MysqlSourceBuilder 对象
        MySqlSourceBuilder<String> builder = MySqlSource.<String>builder()
                .hostname(mysqlHostname)
                .port(mysqlPort)
                .username(mysqlUsername)
                .password(mysqlPasswd)
                .deserializer(jsonDebeziumDeserializationSchema);

        // 根据方法的 option 参数做不同的初始化操作，返回不同的 MysqlSource 对象
        switch (option) {
            case "dim":
                String[] dimTables = new String[]{"tms.user_info",
                        "tms.user_address"};
                return builder
                        .databaseList("tms")
                        .tableList(dimTables)
                        .startupOptions(StartupOptions.initial())
                        .serverId(serverId)
                        .build();
            case "dwd":
                String[] dwdTables = new String[]{"tms.order_info",
                        "tms.order_cargo",
                        "tms.transport_task",
                        "tms.order_org_bound"};
                return builder
                        .databaseList("tms")
                        .tableList(dwdTables)
                        .startupOptions(StartupOptions.initial())
                        .serverId(serverId)
                        .build();
        }
        return null;
    }


}
