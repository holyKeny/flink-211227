package com.atguigu.chapter5.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author lx
 * @date 2022/5/29 - 1:43
 */
public class Flink_Source_Kafka_New2 {
    public static void main(String[] args) throws Exception {
        //1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setTopics("sensor")
                .setGroupId("1227")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        //2.从Kafka消费数据
        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source");

        //3.按照空格切分字符串
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //按照空格切分
                String[] words = value.split(" ");
                //遍历单词
                for (String word : words) {
                    //用采集器收集二元组(word,1)
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        //4.按照word进行分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //5.对二元组的第二个元素求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        //6.打印结果
        result.print();

        //7.提交Job
        env.execute();
    }
}
