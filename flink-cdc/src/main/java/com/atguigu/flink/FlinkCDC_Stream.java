package com.atguigu.flink;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class FlinkCDC_Stream {
    public static void main(String[] args) throws Exception {
        // 1 创建 Flink 运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("converters", "dateConverters");
        properties.setProperty("dateConverters.type", "com.atguigu.tms.realtime.util");

        // 2 创建 mysqlSource
        MySqlSource<String> mysqlSource = MySqlSource
                .<String>builder()
                .hostname("master")
                .port(3306)
                .databaseList("tms")
                .tableList("tms.user_info")
                .username("root")
                .password("000000")
                .debeziumProperties(properties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        env
                .fromSource(
                        mysqlSource,
                        WatermarkStrategy.noWatermarks(),
                        "MySQLSource"
                )
                .print();

        env.execute();
    }
}
