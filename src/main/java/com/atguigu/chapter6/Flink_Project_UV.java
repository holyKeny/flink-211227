package com.atguigu.chapter6;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * @author lx
 * @date 2022/5/31 - 9:36
 */
public class Flink_Project_UV {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从文件获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        //3.并将数据转换为("uv",userID)
        // 这么做的目的是,可以使用uv进行keyBy重分区,即使多并行度,key=uv的数据还是只会去往一个分区.只单独统计一个分区,会来的方便很多
        SingleOutputStreamOperator<Tuple2<String, Long>> uvToUserId = streamSource.flatMap(new RichFlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(",");
                UserBehavior behavior = new UserBehavior(
                        Long.valueOf(split[0]),
                        Long.valueOf(split[1]),
                        Integer.valueOf(split[2]),
                        split[3],
                        Long.valueOf(split[4]));
                if ("pv".equals(behavior.getBehavior())) {
                    out.collect(Tuple2.of("uv", behavior.getUserId()));
                }
            }
        });

        //4.对uvToUserId流中的数据做KeyBy,就是确保将"uv"这个Key的数据都发往一个分区做去重统计
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = uvToUserId.keyBy(0);

        //5.对分区后的数据进行去重,求网站独立访客数
        SingleOutputStreamOperator<Integer> uv = keyedStream.process(new KeyedProcessFunction<Tuple, Tuple2<String, Long>, Integer>() {
            //用于存放独立访客数的集合
            HashSet<Long> userIds = new HashSet<>();

            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Integer> out) throws Exception {
                userIds.add(value.f1);
                out.collect(userIds.size());
            }
        });

        //打印独立访客数
        uv.print("uv:");

        env.execute();

    }
}
