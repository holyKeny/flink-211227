package com.atguigu.chapter5.source;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author lx
 * @date 2022/5/29 - 15:17
 */
public class Flink_Source_Custom {
    public static void main(String[] args) throws Exception {
        //1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //3.从自定义数据源获取数据
        DataStreamSource<WaterSensor> streamSource = env.addSource(new MySource());

        streamSource.print();

        env.execute();
    }

    //2.自定义数据源
    //指定泛型: 泛型含义是数据源生成的数据类型
    public static class MySource implements SourceFunction<WaterSensor>{

        private Random random = new Random();
        private Boolean isRunning = true;

        //生成数据方法,通过SourceContext对象将数据发送出去,发送至下游
        @Override
        public void run(SourceContext ctx) throws Exception {
            while (true){
                ctx.collect(new WaterSensor("sensor"+random.nextInt(100),System.currentTimeMillis(),random.nextInt(1000)));
                Thread.sleep(1000);
            }
        }

        //用于取消数据发送的方法,优雅的关闭
        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
