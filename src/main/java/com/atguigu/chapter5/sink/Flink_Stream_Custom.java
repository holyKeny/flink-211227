package com.atguigu.chapter5.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author lx
 * @date 2022/5/31 - 0:03
 */
public class Flink_Stream_Custom {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为POJO
        SingleOutputStreamOperator<WaterSensor> pojo = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] fields = value.split(" ");
                return new WaterSensor(fields[0], Long.valueOf(fields[1]), Integer.valueOf(fields[2]));
            }
        });

        //4.将数据按照自定义Sink写出
        pojo.addSink(new MySink());

        //5.提交Job
        env.execute();
    }

    //继承富函数,使用声明周期方法创建连接与关闭连接
    private static class MySink extends RichSinkFunction<WaterSensor>{

        private Connection connection = null;
        private PreparedStatement ps = null;
        /**
         * 声明周期方法,Task开始时,每个并行度有且仅执行一次
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            //获取连接
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "123456");
            //获取预编译对象,并添加占位符
            ps = connection.prepareStatement("insert into watersensor values (?,?,?)");
        }

        /**
         * 声明周期方法,Task结束后,每个并行度有且仅执行一次
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            ps.close();
            connection.close();
        }

        //写出数据的业务模块
        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            //为占位符赋值
            ps.setString(1, value.getId());
            ps.setLong(2,value.getTs());
            ps.setInt(3,value.getVc());

            //执行预编译对象
            ps.execute();
        }
    }
}
