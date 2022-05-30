package com.atguigu.chapter5.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author lx
 * @date 2022/5/30 - 3:21
 */
public class Flink_Transform_FlatMap {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.利用flatMap算子将每一行中每一个单词取出来
        SingleOutputStreamOperator<String> result = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //按照空格切分单词
                String[] words = value.split(" ");
                //遍历单词数组,并用采集器收集,通过采集器将数据发送出去
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //4.打印
        result.print();

        //5.执行Job
        env.execute();
    }
}
