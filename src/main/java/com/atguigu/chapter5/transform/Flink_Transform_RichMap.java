package com.atguigu.chapter5.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lx
 * @date 2022/5/30 - 1:02
 */
public class Flink_Transform_RichMap {
    public static void main(String[] args) throws Exception {
        //1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        //2.从文件获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");

        //3.使用Map,将数据转换成WaterSensor对象
        SingleOutputStreamOperator<WaterSensor> result = streamSource.map(new MyMapFunction());

        //4.输出结果
        result.print("print:");

        //5.执行Job
        env.execute();
    }

    //4.创建静态内部类,继承RichMapFunction(富函数)
    private static class MyMapFunction extends RichMapFunction<String, WaterSensor>{
        /**
         * 生命周期方法,在当前Task开始时最先被调用,每个并行度上调用一次
         * 说白了就是每个subTask都会执行一次open方法
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open...");
        }

        /**
         * 生命周期方法,在当前Task结束后,每个并行度调用一次
         * 特殊情况:从文件读取数据,每个并行度执行2次
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            System.out.println("close...");
        }

        /**
         * Map算子的写映射逻辑的方法
         * 每一条数据(一个元素)才执行一次
         * @param value
         * @return
         * @throws Exception
         */
        @Override
        public WaterSensor map(String value) throws Exception {
            System.out.println("伴随subTask名: "+ getRuntimeContext().getTaskNameWithSubtasks());
            System.out.println(getRuntimeContext().getTaskName());
            //切分字符串
            String[] words = value.split(" ");
            //返回WaterSensor对象
            return new WaterSensor(words[0],Long.valueOf(words[1]),Integer.valueOf(words[2]));
        }
    }
}
