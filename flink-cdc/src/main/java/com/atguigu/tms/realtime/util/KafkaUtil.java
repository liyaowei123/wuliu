package com.atguigu.tms.realtime.util;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic.EXACTLY_ONCE;

public class KafkaUtil {

    private static final String DEFAULT_TOPIC = "default_topic";

    /**
     * 指定 topic 获取 FlinkKafkaProducer 实例
     *
     * @param topic 主题
     * @param args  命令行参数数组
     * @return FlinkKafkaProducer 实例
     */
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic, String[] args) {
        // 创建配置对象
        Properties producerProp = new Properties();
        // 将命令行参数对象封装为 ParameterTool 类对象
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // 提取命令行传入的 key 为 topic 的配置信息，并将默认值指定为方法参数 topic
        // 当命令行没有指定 topic 时，会采用默认值
        topic = parameterTool.get("topic", topic);
        // 如果命令行没有指定主题名称且默认值为 null 则抛出异常
        if (topic == null) {
            throw new IllegalArgumentException("主题名不可为空：命令行传参为空且没有默认值!");
        }

        // 获取命令行传入的 key 为 bootstrap-servers 的配置信息，并指定默认值
        String bootstrapServers = parameterTool.get(
                "bootstrap-severs", "master:9092, worker01:9092, worker02:9092");
        // 获取命令行传入的 key 为 transaction-timeout 的配置信息，并指定默认值
        String transactionTimeout = parameterTool.get(
                "transaction-timeout", 15 * 60 * 1000 + "");
        // 设置 Kafka 连接的 URL
        producerProp.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // 设置 Kafka 事务超时时间
        producerProp.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout);

        // 内部类中使用但未声明的局部变量必须在内部类代码段之前明确分配
        String finalTopic = topic;
        return new FlinkKafkaProducer<String>(
                DEFAULT_TOPIC,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String jsonStr, @Nullable Long timestamp) {
                        return new ProducerRecord<byte[], byte[]>(finalTopic, jsonStr.getBytes());
                    }
                },
                producerProp,
                EXACTLY_ONCE);
    }
}

