package com.atguigu.chapter6;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lx
 * @date 2022/5/31 - 8:47
 */
public class Flink_Project_PV {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        //3.将数据转换为JavaBean
        SingleOutputStreamOperator<UserBehavior> userBehavior = streamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] fields = value.split(",");
                return new UserBehavior(Long.valueOf(fields[0]), Long.valueOf(fields[1]), Integer.valueOf(fields[2]), fields[3], Long.valueOf(fields[4]));
            }
        });

        //4.过滤出PV的数据
        SingleOutputStreamOperator<UserBehavior> pvStream = userBehavior.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });

        //5.将数据转换为(pv,1),方便后面求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> pvToOne = pvStream.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return Tuple2.of("pv", 1);
            }
        });

        //6.先用keyBy将流转换为keyedStream,再使用滚动聚合算子sum
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = pvToOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //7.对二元组的第二个元素求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        //8.打印结果
        result.print();

        //9.提交job
        env.execute();
    }
}
