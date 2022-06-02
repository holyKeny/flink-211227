package com.atguigu.chapter6;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author lx
 * @date 2022/5/31 - 9:09
 */
public class Flink_Project_PV_Process {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");


        //3.使用process完成pv的统计
        SingleOutputStreamOperator<Tuple2<String, Integer>> pvSum = streamSource.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            private Integer count = 0;

            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(",");

                if ("pv".equals(split[3])) {
                    count++;
                    out.collect(Tuple2.of("pv", count));
                }
            }
        });

        //4.打印结果
        pvSum.print();

        //5.提交Job
        env.execute();
    }
}
