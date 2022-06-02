package com.atguigu.chapter7;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author lx
 * @date 2022/6/2 - 0:53
 */
public class Flink_Non_KeyBy_Window {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(5);

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

        //对流直接进行开窗,无论该流并行度设置为几,该窗口的并行度始终为1,所有数据都进入到该窗口内
        AllWindowedStream<Tuple2<String, Long>, TimeWindow> window = wordToOneDStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)));

        //打印窗口内的所有元素
        window.process(new ProcessAllWindowFunction<Tuple2<String, Long>, String, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                for (Tuple2<String, Long> element : elements) {
                    out.collect(element.toString());
                }
            }
        }).print();

        env.execute();
    }
}
