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
 * @date 2022/5/26 - 20:00
 *
 *      有界流
 */
public class Flink02_Stream_WordCount {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //将并行度设置为1,方便观察
        // 使用单核进行运算,默认是系统核数
        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("hdfs://hadoop102:8020/input");

        /**
         * 首先flatMap ,按照空格切分,然后将单词转换成Tuple2元组(word,1)
         * 其次ReduceByKey(1.先将相同的单词聚合到一块 2.再做累加)
         * 最后把结果输出到控制台
         */
        //3.按照空格切分,然后组成Tuple2元组(word,1)
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                //按照空格切分
                String[] words = s.split(" ");
                //遍历words,将word转换为二元组
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });

        //4.将相同的单词聚合到一起
        //按照二元组的第一个元素进行聚合
        KeyedStream<Tuple2<String, Integer>, String> keyBy = wordToOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                //按照二元组的第一个元素进行聚合
                return stringIntegerTuple2.f0;
            }
        });

        //5.对同组内的二元组的第二个元素做累加计算
        //Java中二元组的下标从0开始,Scala的二元组下标从1开始
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyBy.sum(1);

        //6.打印结果
        result.print();


        //7.执行代码
        env.execute();
    }
}
