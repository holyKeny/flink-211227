package com.atguigu.chapter5.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author lx
 * @date 2022/5/30 - 17:29
 */
public class Flink_Transform_Connect {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource1 = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> streamSource2 = env.socketTextStream("hadoop102", 9998);

        //3.合并两个流
        ConnectedStreams<String, String> connect = streamSource1.connect(streamSource2);


        //4.对合并后的流做map
        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<String, String, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value + "map1";
            }

            @Override
            public String map2(String value) throws Exception {

                return value + "map2";
            }
        });

        map.print();

        env.execute();
    }
}
