package com.atguigu.chapter5.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lx
 * @date 2022/5/30 - 13:20
 */
public class Flink_Transform_shuffle {
    public static void main(String[] args) throws Exception {
        //创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        //从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //创建一个中间结果
        SingleOutputStreamOperator<String> map = streamSource.map(r -> r).setParallelism(2);

        //将map和print并行度都设置为2,组合成一个Task,将Task中2个分区的数据发往shuffle后的4个分区内
        map.print("原始分区:").setParallelism(2);

        //使用shuffle进行重新分区
        DataStream<String> shuffle = map.shuffle();

        //打印shuffle的结果,查看之前2个分区的数据都发往了shuffle后的哪些分区
        shuffle.print("shuffle");

        env.execute();
    }
}
