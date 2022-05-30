package com.atguigu.chapter5.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author lx
 * @date 2022/5/30 - 18:44
 */
public class Flink_Stream_Max_MaxBy {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为POJO
        SingleOutputStreamOperator<WaterSensor> pojo = streamSource.process(new ProcessFunction<String, WaterSensor>() {
            @Override
            public void processElement(String value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //按照空格切分
                String[] fields = value.split(" ");
                //使用采集器将数据发送至下游
                out.collect(new WaterSensor(fields[0], Long.valueOf(fields[1]), Integer.valueOf(fields[2])));
            }
        });

        //4.对数据按照POJO的id进行分组聚合
        KeyedStream<WaterSensor, Tuple> keyBy = pojo.keyBy("id");

        /**
         *  需求: 求VC值最大的POJO对象
         *  三种滚动聚合算子的差异
         *      max(): 对于输入的数据,只保留vc的最大值(最新)状态,id,ts字段不更新,使用第一条数据的id,ts
         *           例如 端口发送数据为,
         *                 a1 1 3
         *                 a1 2 4
         *                 a1 4 5
         *           此时使用只保留  a1 1 5
         *      maxBy("vc", true): 对于输入的数据,只保留vc为最大值的最早的那一条数据.
         *          例如 端口发送数据为,假设此时 vc=3 就是最大值:
         *                 a1 2 3
         *                 a1 3 3
         *                 a1 4 3
         *          此时使用只保留  a1 2 3
         *      maxBy("vc", false): 对于输入的数据,只保留vc为最大值的最新的那一条数据.
         *          例如 端口发送数据为,假设此时 vc=3 就是最大值:
         *                 a1 2 3
         *                 a1 3 3
         *                 a1 4 3
         *          此时使用只保留  a1 4 3
         */
        SingleOutputStreamOperator<WaterSensor> max = keyBy.max("vc");
        SingleOutputStreamOperator<WaterSensor> maxBy1 = keyBy.maxBy("vc", true);
        SingleOutputStreamOperator<WaterSensor> maxBy2 = keyBy.maxBy("vc", false);

        //6.打印结果
        max.print("max: ");
        maxBy1.print("maxBy1: ");
        maxBy2.print("maxBy2: ");

        //7.提交Job
        env.execute();

    }
}
