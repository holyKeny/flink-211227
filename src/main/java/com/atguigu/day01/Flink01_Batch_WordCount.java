package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author lx
 * @date 2022/5/26 - 17:08
 */
public class Flink01_Batch_WordCount {
    public static void main(String[] args) throws Exception {
        //1.创建批处理环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.从文件读取数据
        DataSource<String> dataSource = env.readTextFile("input/words.txt");

        /**
         * 首先flatMap ,按照空格切分,然后将单词转换成Tuple2元组(word,1)
         * 其次ReduceByKey(1.先将相同的单词聚合到一块 2.再做累加)
         * 最后把结果输出到控制台
         */
        //3.按照空格切分,然后组成Tuple2元组(word,1)
        FlatMapOperator<String, Tuple2<String, Integer>> wordToOne = dataSource.flatMap(new MyFlatMap());

        //4.对二元组的第一个元素进行分组
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOne.groupBy(0);

        //5.将单词个数进行累加,累加二元组的第二个元素
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);

        result.print();
    }

    //两个泛型
    //第1个泛型,是输入数据的类型
    //第2个泛型,是输出数据的类型
    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {

        /**
         * @param value     输入的数据
         * @param collector 采集器:将数据发送至下游,用来存放返回结果的
         *                  因为下面的方法是void类型
         * @throws Exception
         */
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
            //按照空格切分
            String[] words = value.split(" ");
            //遍历每个单词
            for (String word : words) {
                //collect方法收集需要输出的数据
                //collector.collect(new Tuple2<>(word,1));
                collector.collect(Tuple2.of(word,1));
            }
        }
    }
}
