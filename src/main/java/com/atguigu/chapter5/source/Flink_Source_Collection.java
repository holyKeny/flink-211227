package com.atguigu.chapter5.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author lx
 * @date 2022/5/28 - 22:42
 */
public class Flink_Source_Collection {
    public static void main(String[] args) throws Exception {
        //1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // TODO 从集合读取数据
        List<String> list = Arrays.asList("1", "2", "3");
        DataStreamSource<String> streamSource = env.fromCollection(list);

        //读取元素
        //DataStreamSource<String> streamSource = env.fromElements("1", "2", "#");

        //打印结果
        streamSource.print();

        //提交job
        env.execute();
    }
}
