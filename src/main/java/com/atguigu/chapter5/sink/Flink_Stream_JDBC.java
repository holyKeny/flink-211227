package com.atguigu.chapter5.sink;

import com.atguigu.bean.WaterSensor;
import com.mysql.jdbc.Driver;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author lx
 * @date 2022/5/31 - 5:46
 */
public class Flink_Stream_JDBC {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换成POJO
        SingleOutputStreamOperator<WaterSensor> pojo = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] fields = value.split(" ");
                return new WaterSensor(fields[0], Long.valueOf(fields[1]), Integer.valueOf(fields[2]));
            }
        });

        //4.使用Jdbc.sink将数据写出到Mysql

        //创建Jdbc.sink对象
        SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink(
                "insert into watersensor values(?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    //使用预编译对象,为占位符赋值
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        preparedStatement.setString(1, waterSensor.getId());
                        preparedStatement.setLong(2, waterSensor.getTs());
                        preparedStatement.setInt(3, waterSensor.getVc());
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1)           //每一条数据执行一次写操作
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(Driver.class.getName())
                        .withUrl("jdbc:mysql://hadoop102:3306/test?useSSL=false")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );

        //5.执行Sink操作
        pojo.addSink(jdbcSink);

        //6.提交Job
        env.execute();
    }
}
