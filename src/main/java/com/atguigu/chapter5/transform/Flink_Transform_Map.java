package com.atguigu.chapter5.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lx
 * @date 2022/5/30 - 2:10
 */
public class Flink_Transform_Map {
    public static void main(String[] args) throws Exception {
        //1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换成WaterSensor对象
        SingleOutputStreamOperator<WaterSensor> result = streamSource.map(line -> {
            String[] elems = line.split(" ");
            return new WaterSensor(elems[0],Long.valueOf(elems[1]),Integer.valueOf(elems[2]));
        });

        //4.打印结果
        result.print();

        //5.提交Job
        env.execute();
    }

    /**
     * 创建静态内部类,实现MapFunction接口
     * 两个泛型:第一个是从端口获取的数据类型,第二个是要返回的数据类型
     */
    private static class MyMapFunction implements MapFunction<String, WaterSensor>{

        //实现映射逻辑的方法
        @Override
        public WaterSensor map(String value) throws Exception {
            //将数据按照空格切分
            String[] elems = value.split(" ");
            //返回WaterSensor对象
            return new WaterSensor(elems[0],Long.valueOf(elems[1]),Integer.valueOf(elems[2]));
        }
    }
}
