package com.atguigu.chapter5.source;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 * @author lx
 * @date 2022/5/29 - 22:24
 */
public class Flink_Source_Custom02 {
    public static void main(String[] args) throws Exception {
        //1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);


        //3.从自定义数据源获取数据
        DataStreamSource<WaterSensor> streamSource = env.addSource(new MySource()).setParallelism(3);

        //4.输出数
        streamSource.print();

        //5.执行Job
        env.execute();
    }

    //2.自定义数据源
    private static class MySource implements ParallelSourceFunction<WaterSensor>{

        private Socket hadoop102 = null;

        //生成数据的方法,通过ctx对象采集数据,并发送给下游
        @Override
        public void run(SourceContext ctx) throws Exception {
            //从一个Socket读取数据
            hadoop102 = new Socket("hadoop102", 9999);

            BufferedReader reader = new BufferedReader(new InputStreamReader(hadoop102.getInputStream(),"UTF-8"));
            String line = null;
            while ((line = reader.readLine())!=null){
                String[] elems = line.split(" ");
                ctx.collect(new WaterSensor(elems[0], System.currentTimeMillis(), Integer.valueOf(elems[1])));
            }
        }

        //页面端点击cancel job,优雅的关闭,就会执行该方法.
        @Override
        public void cancel() {
            try {
                hadoop102.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
