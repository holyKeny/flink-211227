package com.atguigu.chapter5.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author lx
 * @date 2022/5/30 - 22:10
 */
public class Flink_Stream_RedisSink {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换成WaterSensor
        SingleOutputStreamOperator<WaterSensor> pojo = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] fields = value.split(" ");
                return new WaterSensor(fields[0], Long.valueOf(fields[1]), Integer.valueOf(fields[2]));
            }
        });

        //4.将数据写出到Redis
        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6000)
                .build();

        DataStreamSink<WaterSensor> sink = pojo.addSink(new RedisSink<>(flinkJedisPoolConfig, new RedisMapper<WaterSensor>() {

            /**
             * 选择要写入Redis要执行的命令,例如SET,HSET,SADD...
             * @return
             */
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.SET);

                // 如果是HSET操作,那么需要在这里传入写出Redis的主key
                // return new RedisCommandDescription(RedisCommand.HSET,"RedisKey")
            }

            /**
             * 提取数据作为写入Redis的Key.
             * 特殊情况:如果是HSET操作,这里提取的Key是作为Redis中Hash类型数据的Key,不是Redis的主Key
             * @param data
             * @return
             */
            @Override
            public String getKeyFromData(WaterSensor data) {
                return data.getId();
            }

            @Override
            public String getValueFromData(WaterSensor data) {
                return JSONObject.toJSONString(data);
            }
        }));

        //5.执行Job
        env.execute();
    }
}