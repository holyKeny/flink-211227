package com.atguigu.chapter7watermark;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author lx
 * @date 2022/6/2 - 15:13
 */
public class Flink_WaterMark_Monotonously {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);


        //3.将数据转换为WaterSensor,方便后续使用JavaBean中的时间戳作为事件时间
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");

                return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
            }
        });

        //4.为流上的数据设置WaterMark
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorStream.assignTimestampsAndWatermarks(WatermarkStrategy
                //指定0延迟单调递增的WaterMark
                .<WaterSensor>forMonotonousTimestamps()
                //指定字段为事件时间字段
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        //指定事件时间字段,需要乘以1000,让他变成秒
                        return element.getTs() * 1000;
                    }
                })
        );

        //5.将相同Key的数据聚集到一起
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorSingleOutputStreamOperator.keyBy("id");

        //6.开启一个基于事件时间的滚动窗口,窗口大小为5秒
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        //7.打印窗口内的详细信息
        window.process(new ProcessWindowFunction<WaterSensor, String, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg = "窗口大小为: [" + context.window().getStart()/1000 + " , " + context.window().getEnd()/1000 + ")" + "\n" +
                        "有" + elements.spliterator().estimateSize() + "个元素";

                out.collect(msg);
            }
        }).print();

        env.execute();


    }
}
