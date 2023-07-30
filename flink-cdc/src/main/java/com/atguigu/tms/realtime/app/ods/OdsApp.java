package com.atguigu.tms.realtime.app.ods;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.util.CreateEnvUtil;
import com.atguigu.tms.realtime.util.DateFormatUtil;
import com.atguigu.tms.realtime.util.KafkaUtil;
import com.esotericsoftware.minlog.Log;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

public class OdsApp {
    public static void main(String[] args) throws Exception {

        // TODO 1. 初始化流处理环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // 并行度设置，部署时应注释，通过 args 指定全局并行度
        env.setParallelism(1);

        // TODO 2. 处理维度数据
        String dimOption = "dim";
        String dimServerId = "6020";
        String dimSourceName = "ods_dim_source";
        sinkToKafka(dimOption, dimServerId, dimSourceName, env, args);

        // TODO 3. 处理事实数据
        String dwdOption = "dwd";
        String dwdServerId = "6030";
        String dwdSourceName = "ods_dwd_source";
        sinkToKafka(dwdOption, dwdServerId, dwdSourceName, env, args);

        env.execute();
    }

    public static void sinkToKafka(
            String option, String serverId, String sourceName, StreamExecutionEnvironment env, String[] args) {
        // 1. 读取数据
        MySqlSource<String> mysqlSource = CreateEnvUtil.getJSONSchemaMysqlSource(option, serverId, args);
        DataStreamSource<String> source = env
                .fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), sourceName)
                .setParallelism(1);

        // 2. ETL
        // 获取统计日期
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String mockDate = parameterTool.get("mock_date");
        SingleOutputStreamOperator<String> flatMappedStream =
                source.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String jsonStr, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            if (jsonObj.getJSONObject("after") != null
                                    && !jsonObj.getString("op").equals("d")) {
                                Long ts = jsonObj.getLong("ts_ms");
                                if (mockDate != null) {
                                    String curDate = DateFormatUtil.toYmdHms(ts);
                                    String fixedDate = mockDate + curDate.substring(10);
                                    Long fixedTs = DateFormatUtil.toTs(fixedDate);
                                    jsonObj.put("ts", fixedTs);
                                } else {
                                    jsonObj.put("ts", ts);
                                }
                                jsonObj.remove("ts_ms");
                                out.collect(jsonObj.toJSONString());
                            }
                        } catch (JSONException jsonException) {
                            jsonException.printStackTrace();
                        }
                    }
                }).setParallelism(1);

        // 3. 按照主键分组，避免数据倾斜
        KeyedStream<String, String> keyedStream = flatMappedStream.keyBy(
                new KeySelector<String, String>() {
                    @Override
                    public String getKey(String jsonStr) {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        return jsonObj.getJSONObject("after").getString("id");
                    }
                }
        );

        // 4. 写入 Kafka 对应主题
        String topic = "tms_ods";
        FlinkKafkaProducer<String> kafkaProducer = KafkaUtil.getKafkaProducer(topic, args);
        keyedStream
                .addSink(kafkaProducer);
    }
}
