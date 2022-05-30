package com.atguigu.chapter5.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lx
 * @date 2022/5/30 - 17:00
 */
public class Flink_Transform_Reduce {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换成WaterSensor对象
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] fields = value.split(" ");
                return new WaterSensor(fields[0], Long.valueOf(fields[1]), Integer.valueOf(fields[2]));
            }
        });

        //4.对id进行分组聚合
        KeyedStream<WaterSensor, String> keyBy = map.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        //5.求每个id的vc最大的WaterSensor对象
        SingleOutputStreamOperator<WaterSensor> maxVC = keyBy.reduce(new ReduceFunction<WaterSensor>() {

            /**
             *
             * @param value1    value1保存上一次 聚合后的结果
             * @param value2    value2是每一条进来的数据,需要聚合的数据
             * @return          return的是聚合后的结果
             * @throws Exception
             */

            //聚合逻辑
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                return new WaterSensor(value1.getId(), value2.getTs(), Math.max(value1.getVc(), value2.getVc()));
            }
        });

        maxVC.print();

        env.execute();
    }
}
