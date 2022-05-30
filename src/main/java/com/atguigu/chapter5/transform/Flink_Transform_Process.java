package com.atguigu.chapter5.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @author lx
 * @date 2022/5/30 - 14:59
 */
public class Flink_Transform_Process {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //需求,做WordCount

        //3.对数据按空格进行切分,并将单词转换成Tuple2(word,1)类型
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = streamSource.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                //按照空格切分数据
                String[] words = value.split(" ");
                //遍历单词数组
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        //4.对数据按照word进行分组聚合
        KeyedStream<Tuple2<String, Integer>, String> keyBy = wordToOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //5.对KeyBy后的数据按照Key相同的数据进行求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyBy.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            //创建缓存,用于累加不同Key的Value值
            HashMap<String, Integer> map = new HashMap<>();

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                if (map.containsKey(value.f0)) {
                    Integer sum = map.get(value.f0);
                    sum += value.f1;
                    map.put(value.f0, sum);
                } else {
                    map.put(value.f0, value.f1);
                }

                out.collect(Tuple2.of(value.f0, map.get(value.f0)));
            }
        }).setParallelism(2);

        //6.打印求和后的结果
        result.print().setParallelism(2);

        //7.提交job
        env.execute();
    }
}
