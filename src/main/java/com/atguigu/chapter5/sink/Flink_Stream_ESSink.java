package com.atguigu.chapter5.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.checkerframework.checker.units.qual.A;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author lx
 * @date 2022/5/30 - 22:51
 */
public class Flink_Stream_ESSink {
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

        //4.将数据写出到ES
        //指定ES连接地址
        List<HttpHost> httpHosts = Arrays.asList(new HttpHost("hadoop102", 9200));

        //创建ElasticsearchSink.Builder
        ElasticsearchSink.Builder<WaterSensor> builder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<WaterSensor>() {
            /**
             *
             * @param element   数据
             * @param ctx
             * @param indexer   用户将多个删除、索引或更新请求添加到 {@link RequestIndexer} 以准备将它们发送到 Elasticsearch 集群。
             */
            @Override
            public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {
                //指定要插入的索引名,类型名,文档id到写请求
                IndexRequest indexRequest = new IndexRequest("1227_flink", "_doc", "1001");
                //将要写出的JSON数据放入到写请求中
                indexRequest.source(JSONObject.toJSONString(element), XContentType.JSON);
                //添加写请求
                indexer.add(indexRequest);
            }
        });

        //设置单条数据刷写到ES
        //生产环境不建议使用这个参数,容易把ES冲挂掉
        builder.setBulkFlushMaxActions(1);

        pojo.addSink(builder.build());

        //执行Job
        env.execute();
    }
}
