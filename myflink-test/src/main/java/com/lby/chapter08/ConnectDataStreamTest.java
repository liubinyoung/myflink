package com.lby.chapter08;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author ：lby
 * @date ：Created at 2022/4/17 20:59
 * @desc ：通过connect算子进行流的合并。
 * 只能两条流相合并，两条流的数据类型可以不同。合并之后返回的数据类型是ConnectedStream。
 */
public class ConnectDataStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStreamSource<Integer> sourceDS1 = streamEnv.fromElements(1, 2, 3);
        DataStreamSource<String> sourceDS2 = streamEnv.fromElements("one", "two", "three");
        // connect方法返回值是ConnectedStreams类型；
        ConnectedStreams<Integer, String> connectStream = sourceDS1.connect(sourceDS2);
        // ConnectedStreams调用process方法，需要传入一个CoProcessFunction(抽象类)类型参数；
        connectStream.process(new CoProcessFunction<Integer, String, String>() {
            @Override
            public void processElement1(Integer integer, Context context, Collector<String> collector) throws Exception {
                collector.collect("DS1值为：" + integer.toString());
            }

            @Override
            public void processElement2(String s, Context context, Collector<String> collector) throws Exception {
                collector.collect("DS2值为：" + s);
            }
        }).print();

        streamEnv.execute();
    }
}
