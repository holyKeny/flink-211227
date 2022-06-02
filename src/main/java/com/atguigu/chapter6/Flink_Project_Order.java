package com.atguigu.chapter6;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @author lx
 * @date 2022/5/31 - 15:06
 */
public class Flink_Project_Order {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        //2.从文件获取数据,并转换成样例类
        SingleOutputStreamOperator<OrderEvent> orderEvent = env.socketTextStream("hadoop102",9999).map(new RichMapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(Long.valueOf(split[0]), split[1], split[2], Long.valueOf(split[3]));
            }
        });

        SingleOutputStreamOperator<TxEvent> txEvent = env.socketTextStream("hadoop102",9999).map(new RichMapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent(split[0], split[1], Long.valueOf(split[2]));
            }
        });

        //3.使用connect连接两个流,将两个流合并到一个流上
        ConnectedStreams<OrderEvent, TxEvent> connect = orderEvent.connect(txEvent);

        //4.对connect使用KeyBy,对元素进行分组聚合,将相同txId的数据发往到一个subTask上进行process处理
        //这么做的原因是:如果有多个并行度,在不使用KeyBy,相同Key的数据可能会在不同分区上,这样在后续关联的时候,数据会丢失
        ConnectedStreams<OrderEvent, TxEvent> keyBy = connect.keyBy("txId", "txId");

        SingleOutputStreamOperator<String> joinResult = keyBy.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {

            //每个并行实例中创建缓存,用于缓存暂时没有关联上的数据
            HashMap<String, OrderEvent> orderMap = new HashMap<>();
            HashMap<String, TxEvent> txMap = new HashMap<>();

            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                if (txMap.containsKey(value.getTxId())) {
                    //说明可以关联上
                    out.collect("订单: " + value + "对账成功");
                    //删除被关联后的数据,释放内存空间
                    txMap.remove(value.getTxId());
                } else {
                    //没有关联上,就将数据添加orderMap缓存
                    orderMap.put(value.getTxId(), value);
                }
            }

            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                if (orderMap.containsKey(value.getTxId())) {
                    //说明可以关联上
                    out.collect("订单: " + orderMap.get(value.getTxId()) + "对账成功");
                    //删除被关联后的数据,释放内存空间
                    orderMap.remove(value.getTxId());
                } else {
                    //没有关联上,添加到缓存
                    txMap.put(value.getTxId(), value);
                }
            }
        }).setParallelism(2);

        //5.打印对账成功的结果
        joinResult.print();


        //6.提交Job
        env.execute();
    }
}
