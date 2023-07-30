package com.atguigu.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDC_SQL {
    public static void main(String[] args) throws Exception {
        // TODO 1. 准备环境
        // 1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.2 表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 创建动态表
        tableEnv.executeSql("CREATE TABLE user_info (\n" +
                "id INT,\n" +
                "login_name STRING,\n" +
                "nick_name STRING,\n" +
                "passwd STRING,\n" +
                "real_name STRING,\n" +
                "phone_num STRING,\n" +
                "email STRING,\n" +
                "head_img STRING,\n" +
                "primary key(id) not enforced\n" +
                ") WITH (" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = 'hadoop102'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = '000000'," +
                "'database-name' = 'tms01'," +
                "'table-name' = 'user_info'" +
                ")");

        tableEnv.executeSql("select * from user_info").print();

        // TODO 3. 执行任务
        env.execute();
    }

}
