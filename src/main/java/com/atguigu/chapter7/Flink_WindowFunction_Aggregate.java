package com.atguigu.chapter7;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author lx
 * @date 2022/6/2 - 0:07
 */
public class Flink_WindowFunction_Aggregate {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将数据转为Tuple
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDStream = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(Tuple2.of(s, 1L));
                }
            }
        });

        //4.对数据按照word进行分组分区
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = wordToOneDStream.keyBy(1);

        //5.对每组数据进行开一个滚动窗口
        WindowedStream<Tuple2<String, Long>, Tuple, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        //6.使用Aggregation增量聚合函数,他与Reduce区别在于,Aggregation可以改变聚合后的数据类型
        window.aggregate(new AggregateFunction<Tuple2<String, Long>, String, String>() {
            /**
             *
             * @return  定义累加器的初始值
             */
            @Override
            public String createAccumulator() {
                return "";
            }

            /**
             *      聚合逻辑
             * @param value         传进窗口内的每条数据
             * @param accumulator   累加器
             * @return
             */
            @Override
            public String add(Tuple2<String, Long> value, String accumulator) {
                return accumulator + value + " ";
            }

            /**
             *
             * @param accumulator   返回累加器结果
             * @return
             */
            @Override
            public String getResult(String accumulator) {
                return accumulator;
            }

            /**
             *  用于会话窗口的多个累加器的聚合
             * @param a
             * @param b
             * @return
             */
            @Override
            public String merge(String a, String b) {
                return null;
            }
        }).print();

        //打印窗口详细信息
        window.process(new ProcessWindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                String msg = "窗口大小为: [" + context.window().getStart() + " , " + context.window().getEnd() + "]" + "\n" +
                        "有" + elements.spliterator().estimateSize() + "个元素";

                out.collect(msg);
            }
        }).print();

        env.execute();
    }
}
