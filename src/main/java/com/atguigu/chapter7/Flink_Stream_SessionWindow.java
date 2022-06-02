package com.atguigu.chapter7;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author lx
 * @date 2022/6/1 - 12:17
 */
public class Flink_Stream_SessionWindow {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        //4.将相同的单词聚合到同一个分区
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = wordToOneDStream.keyBy(0);

        //TODO 5.开启一个基于静态时间的会话窗口
        WindowedStream<Tuple2<String, Long>, Tuple, TimeWindow> sessionWindow = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)));

        //打印窗口内数据的个数
        SingleOutputStreamOperator<String> process = sessionWindow.process(new ProcessWindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                String msg = "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                        + elements.spliterator().estimateSize() + "条数据 ";

                out.collect(msg);
            }
        });

        //打印窗口详细信息
        process.print();

        //对窗口内数据按照二元组第二个元素进行聚合,并打印
        sessionWindow.sum(1).print();


        env.execute();


    }
}
