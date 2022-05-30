package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author lx
 * @date 2022/5/27 - 19:06
 */
public class Flink_Stream_Unbounded_WordCount {
    public static void main(String[] args) throws Exception {
        //1.创建流式运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置单核处理任务
        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.按照空格切分字符串,并转换为(word,1)
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //按照空格切分Value
                String[] words = value.split(" ");
                //遍历单词,并转换成(word,1).使用采集器采集
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        //4.对word进行分组
        KeyedStream<Tuple2<String, Integer>, String> keyBy = wordToOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                //返回二元组第一个元素,因为是以第一个元素进行分组
                return value.f0;
            }
        });

        //5.对分组后的数据,按照二元组的第二个元素聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyBy.sum(1);

        //6.打印结果
        result.print();

        //7.提交Job
        env.execute();
    }
}
