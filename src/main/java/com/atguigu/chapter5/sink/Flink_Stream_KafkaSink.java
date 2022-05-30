package com.atguigu.chapter5.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author lx
 * @date 2022/5/30 - 21:26
 *
 *
 *      将WaterSensor对象转换成JSON字符串写出到kafka
 */
public class Flink_Stream_KafkaSink {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换成POJO,再转换成JSON
        SingleOutputStreamOperator<String> json = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                //按照空格切分
                String[] fields = value.split(" ");
                WaterSensor waterSensor = new WaterSensor(fields[0], Long.valueOf(fields[1]), Integer.valueOf(fields[2]));
                return JSONObject.toJSONString(waterSensor);
            }
        });

        //4.将JSON数据写出到Kafka
        //json.addSink(new FlinkKafkaProducer<String>("hadoop102:9092", "topic_sensor", new SimpleStringSchema()));

        //4.加载Properties配置参数,将Json数据写出到kafka
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        json.addSink(new FlinkKafkaProducer<String>("topic_sensor", new SimpleStringSchema(),properties));

        env.execute();
    }
}
