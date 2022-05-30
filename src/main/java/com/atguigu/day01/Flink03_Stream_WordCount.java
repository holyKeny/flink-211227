package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author lx
 * @date 2022/5/26 - 20:51
 *
 *  无界流
 *  如果读取的数据是从端口发出的,就是无界数据 一行一行处理的
 *  如果是读取的文件,就是有界数据,一行一行处理的
 *  都是一行一行处理
 */
public class Flink03_Stream_WordCount {
    public static void main(String[] args) throws Exception {
        //1.创建流式运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度为1
        env.setParallelism(1);

        //2.读取无界数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.对每行数据做WordCount
        SingleOutputStreamOperator<String> words = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                //对一行数据按空格切分
                String[] words = s.split(" ");
                //遍历数组,取出每一个单词放到采集器中
                for (String word : words) {
                    collector.collect(word);
                }
            }
        })
                .setParallelism(2)
                .slotSharingGroup("group1");

        //4.将单词映射为二元组(word,1)
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });

        //5.按照单词进行分组
        KeyedStream<Tuple2<String, Integer>, String> keyBy = wordToOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                //对word进行分组,即按照二元组的第一个元素进行分组
                return stringIntegerTuple2.f0;
            }
        });

        //6.对分组后的单词,按照二元组的第二个元素累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyBy.sum(1);

        //7打印结果
        result.print("累加结果");

        //8.执行代码
        env.execute();
    }
}
