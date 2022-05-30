package com.atguigu.chapter5.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author lx
 * @date 2022/5/28 - 22:50
 *
 *      Flink 1.13版本之前连接Kafka
 */
public class Flink_Source_Kafka_old {
    public static void main(String[] args) throws Exception {
        //1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从Kafka消费数据
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"1227");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");

        DataStreamSource<String> kafkaSource = env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), prop));

        //消费数据并打印
        kafkaSource.print();

        //提交job
        env.execute();
    }
}
