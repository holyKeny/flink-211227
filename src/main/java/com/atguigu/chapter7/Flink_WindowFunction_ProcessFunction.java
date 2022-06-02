package com.atguigu.chapter7;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
 * @date 2022/6/2 - 0:22
 */
public class Flink_WindowFunction_ProcessFunction {
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


        //6.使用ProcessingFunction全窗口函数,求WordCount
        window.process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow>() {
            /**
             *
             * @param tuple     该窗口的Key
             * @param context   上下文对象
             * @param elements  缓存窗口内数据的集合
             * @param out       采集器
             * @throws Exception
             */
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Long>> out) throws Exception {
                System.out.println("[" + context.window().getStart() + " , " + context.window().getEnd() + "]");
                System.out.println("processFunction...");
                //打印窗口内所有元素
                for (Tuple2<String, Long> element : elements) {
                    out.collect(element);
                }
            }
        }).print();

        env.execute();
    }
}
