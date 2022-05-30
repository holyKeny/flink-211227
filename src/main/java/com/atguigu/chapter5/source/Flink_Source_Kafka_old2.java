package com.atguigu.chapter5.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author lx
 * @date 2022/5/29 - 1:13
 */
public class Flink_Source_Kafka_old2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //配置Kafka参数
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"1227");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        DataStreamSource<String> streamSource = env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), prop));

        //消费数据并打印
        streamSource.print();

        env.execute();
    }
}
