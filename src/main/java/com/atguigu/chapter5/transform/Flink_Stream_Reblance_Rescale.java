package com.atguigu.chapter5.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lx
 * @date 2022/5/30 - 20:26
 */
public class Flink_Stream_Reblance_Rescale {
    public static void main(String[] args) throws Exception {
        //创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(5);

        //从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //创建一个中间结果
        SingleOutputStreamOperator<String> map = streamSource.map(r -> r).setParallelism(2);

        //将map和print并行度都设置为2,组合成一个Task,将Task中2个分区的数据发往shuffle后的4个分区内
        map.print("原始分区:").setParallelism(2);

        //使用不同的重分区算子对数据进行重分区
        DataStream<String> shuffle = map.shuffle();
        DataStream<String> rescale = map.rescale();
        DataStream<String> rebalance = map.rebalance();

        //打印shuffle的结果,查看之前2个分区的数据都发往了shuffle后的哪些分区
        shuffle.print("shuffle");
        rescale.print("rescale");
        rebalance.print("rebalance");

        env.execute();
    }
}
