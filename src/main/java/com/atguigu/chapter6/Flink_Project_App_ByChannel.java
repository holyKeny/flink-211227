package com.atguigu.chapter6;

import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author lx
 * @date 2022/5/31 - 18:56
 */
public class Flink_Project_App_ByChannel {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从自定义源获取数据
        DataStreamSource<MarketingUserBehavior> streamSource = env.addSource(new AppMarketingDataSource());

        //3.将数据转换为二元组(渠道-行为,1)
        SingleOutputStreamOperator<Tuple2<String, Integer>> channelAndBehavior = streamSource.map(new RichMapFunction<MarketingUserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(MarketingUserBehavior value) throws Exception {
                return Tuple2.of(value.getChannel() + "-" + value.getBehavior(), 1);
            }
        });

        //4.分组聚合求各渠道各行为次数
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = channelAndBehavior.keyBy(0);

        //5.按照第二个元素聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        //6.打印
        result.print();

        env.execute();

    }
    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());
                ctx.collect(marketingUserBehavior);
                Thread.sleep(250);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }

}
