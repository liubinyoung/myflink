package com.lby.chapter05;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义分区
 *      偶数分到0号分区，技术分到1号分区
 */
public class CustomPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> sourceDS = streamEnv.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer elem) throws Exception {
                        return elem;
                    }
                });

        sourceDS.print().setParallelism(2);

        streamEnv.execute();
    }
}
