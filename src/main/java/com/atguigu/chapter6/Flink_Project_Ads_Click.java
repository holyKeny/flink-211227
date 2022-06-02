package com.atguigu.chapter6;

import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lx
 * @date 2022/5/31 - 18:38
 */
public class Flink_Project_Ads_Click {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件读取数据,并转换为(省份-广告,1)
        SingleOutputStreamOperator<Tuple2<String, Integer>> adsToOne = env.readTextFile("input/AdClickLog.csv").map(new RichMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                //按照逗号切分数据
                String[] split = value.split(",");
                //转换为JavaBean
                AdsClickLog adsClickLog = new AdsClickLog(Long.valueOf(split[0]), Long.valueOf(split[1]), split[2], split[3], Long.valueOf(split[4]));

                //转换为二元组(省份-广告,1)
                return Tuple2.of(adsClickLog.getProvince() + "-" + adsClickLog.getAdId(), 1);
            }
        });

        //3.分组聚合
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = adsToOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //4.求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        //5.打印
        result.print();

        //6.提交
        env.execute();
    }
}
