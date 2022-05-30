package com.atguigu.chapter5.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lx
 * @date 2022/5/29 - 0:20
 *
 *      Flink 1.13+版本连接Kafka
 */
public class Flink_Source_Kafka_New {
    public static void main(String[] args) throws Exception {
        //1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 从Kafka读数据1.13+

        //创建KafkaSource
        //两个泛型OUT类型都是String,反序列化使用SimpleStringSchema
        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setGroupId("1227")
                .setTopics("sensor")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        //2.创建kafkaStream
        DataStreamSource<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source");


        //3.消费数据并打印
        kafkaStream.print();

        //4.提交Job
        env.execute();
    }
}
