package com.atguigu.chapter5.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lx
 * @date 2022/5/30 - 3:47
 */
public class Flink_Transform_Filter {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.奇数保留,偶数过滤掉
        SingleOutputStreamOperator<String> result = streamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return Integer.valueOf(value) % 2 == 1;
            }
        });

        //4.打印
        result.print();

        //5.提交Job
        env.execute();
    }
}
