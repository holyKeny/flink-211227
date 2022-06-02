package com.atguigu.chapter5.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lx
 * @date 2022/5/30 - 12:44
 */
public class Flink_Transform_KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(4);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<String> map = streamSource.map(r -> r).setParallelism(2);

        //将map和print并行度都设置为2,组合成一个Task,将Task中2个分区的数据发往KeyBy后的4个分区内
        map.print("原始分区:").setParallelism(2);

        //使用KeyBy对相同Key的数据进行分组聚合
        KeyedStream<String, String> keyBy = map.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        });


        //打印keyBy的结果
        keyBy.print();

        env.execute();
    }
}
