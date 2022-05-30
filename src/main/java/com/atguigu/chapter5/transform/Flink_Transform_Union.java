package com.atguigu.chapter5.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lx
 * @date 2022/5/30 - 18:10
 */
public class Flink_Transform_Union {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource1 = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> streamSource2 = env.socketTextStream("hadoop102", 9998);
        DataStreamSource<String> streamSource3 = env.socketTextStream("hadoop102", 9997);

        //3.真正合并三个流
        DataStream<String> union = streamSource1.union(streamSource2, streamSource3);

        //4.同样的,合并后的流,也是对每条数据进行处理,来一条处理一条
        SingleOutputStreamOperator<String> map = union.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "union: " + value;
            }
        }).setParallelism(2);

        //5.打印
        map.print().setParallelism(2);

        env.execute();
    }
}
